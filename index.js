const _ = require('rubico')
const exception = require('@chimaera/exception')
const queryLexer = require('@chimaera/query-lexer')
const uri = require('@chimaera/uri')
const DynamoDB = require('aws-sdk/clients/dynamodb')

const getTableName = config => `${config.env}.${config.prefix}`

const toDynamoAttributeValue = x => {
  if (_.isString(x)) return { S: x }
  if (_.isNumber(x)) return { N: _.toString(x) }
  if (_.isBoolean(x)) return { BOOL: x }
  if (_.isArray(x)) return { L: _.map.sync(toDynamoAttributeValue)(x) }
  if (_.isObject(x)) return { M: _.map.sync(toDynamoAttributeValue)(x) }
  throw new TypeError(`cannot coerce ${_.inspect(x)} to dynamo attribute value`)
}

const fromDynamoAttributeValue = x => {
  const e = new TypeError(
    `cannot extract from dynamo attribute value ${_.inspect(x)}`,
  )
  if (!_.isObject(x)) throw e
  if (_.has('S')(x)) return String(x.S)
  if (_.has('N')(x)) return Number(x.N)
  if (_.has('BOOL')(x)) return Boolean(x.BOOL)
  if (_.has('L')(x)) return _.map.sync(fromDynamoAttributeValue)(x.L)
  if (_.has('M')(x)) return _.map.sync(fromDynamoAttributeValue)(x.M)
  throw e
}

// config {...} => client {...} => () => undefined
const init = config => client => _.flow(
  () => ({
    TableName: getTableName(config),
    KeySchema: [{ AttributeName: '_id', KeyType: 'HASH' }],
    AttributeDefinitions: [{ AttributeName: '_id', AttributeType: 'S' }],
  }),
  x => {
    if (_.get('schema.base.dynamo_capacity')(config) === 'on_demand') {
      x.BillingMode = 'PAY_PER_REQUEST'
    } else {
      x.BillingMode = 'PROVISIONED'
      x.ProvisionedThroughput = {
        ReadCapacityUnits: _.get('schema.base.dynamo_rcu', 5)(config),
        WriteCapacityUnits: _.get('schema.base.dynamo_wcu', 5)(config),
      }
    }
    if (_.has('schema.indexes')(config)) {
      const addedFields = new Set(['_id'])
      for (const index of config.schema.indexes) {
        if (_.size(index.key) < 2) throw new RangeError(`global secondary indexes require two fields`)
        for (const key of index.key) {
          if (addedFields.has(key.name)) continue
          addedFields.add(key.name)
          x.AttributeDefinitions.push({
            AttributeName: key.name,
            AttributeType: (t => {
              if (t === 'string') return 'S'
              if (t === 'number') return 'N'
              if (t === 'binary') return 'B'
              throw new RangeError(`invalid type ${t}`)
            })(key.type),
          })
        }
        if (_.dne(x.GlobalSecondaryIndexes)) x.GlobalSecondaryIndexes = []
        x.GlobalSecondaryIndexes.push({
          IndexName: `${index.key[0].name}-${index.key[1].name}-index`,
          KeySchema: [
            { AttributeName: index.key[0].name, KeyType: 'HASH' },
            { AttributeName: index.key[1].name, KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
          ..._.get('schema.base.dynamo_capacity')(config) === 'on_demand' ? ({
            // BillingMode: 'PAY_PER_REQUEST',
          }) : ({
            // BillingMode: 'PROVISIONED',
            ProvisionedThroughput: {
              ReadCapacityUnits: _.get('dynamo_rcu', 5)(index),
              WriteCapacityUnits: _.get('dynamo_wcu', 5)(index),
            },
          })
        })
      }
    }
    return x
  },
  _.tryCatch(_.flow(
    client.createTable,
    _.diverge(['tableExists', _.diverge({ TableName: getTableName(config) })]),
    _.spread(client.waitFor),
  ), _.switch(_.eq('ResourceInUseException', _.get('name')), _.noop, _.throw)),
)

// config {...} => client {...} => () => undefined
const free = config => client => _.flow(
  _.diverge({ TableName: getTableName(config) }),
  _.tryCatch(_.flow(
    client.deleteTable,
    _.diverge([
      'tableNotExists',
      _.diverge({ TableName: getTableName(config) }),
    ]),
    _.spread(client.waitFor),
  ), _.switch(
    _.eq('ResourceNotFoundException', _.get('name')),
    _.id,
    _.throw,
  )),
  _.noop,
)

// config {...} => client {...} => () => undefined
const ready = config => client => _.flow(
  _.diverge({ TableName: getTableName(config) }),
  _.tryCatch(
    client.describeTable,
    _.switch(
      _.eq('ResourceNotFoundException', _.get('name')),
      _.flow(
        () => uri.encodePublic(config),
        exception.ConnectionNotReady,
        _.throw,
      ),
      _.throw,
    ),
  ),
  _.noop,
)

// config {...} => client {...} => id {...} => resource {...}
const get = config => client => _.flow(
  _.diverge({
    _id: _.get('_id'),
    _source: _.flow(
      _.map(toDynamoAttributeValue),
      _.diverge({ TableName: getTableName(config), Key: _.id }),
      client.getItem,
      _.get('Item'),
    ),
  }),
  _.switch(
    _.has('_source'),
    _.flow(
      _.get('_source'),
      _.map(fromDynamoAttributeValue),
      _.diverge({ _id: _.get('_id'), _source: _.exclude('_id') }),
    ),
    _.flow(
      _.diverge([uri.encodePublic(config), _.get('_id')]),
      _.join('/'),
      exception.ResourceNotFound,
      _.throw,
    ),
  ),
)

// config {...} => client {...} => resource {
//   _id: '1',
//   _source: { a: 1 },
// } => status {...}
const put = config => client => _.flow(
  _.diverge([_.pick('_id'), _.get('_source')]),
  _.spread(Object.assign),
  _.map(toDynamoAttributeValue),
  _.diverge({ TableName: getTableName(config), Item: _.id }),
  client.putItem,
  () => ({ ok: true }),
)

// any => string
const toHash = _.flow.sync(_.inspect, _.sha256, _.slice(0, 10))

// object => string
// { a: 2, c: { d: true } } => """
// set #toHash('a') = :toHash(2),
//     #toHash('c') = :toHash({ d: true })
// """
const toDynamoUpdateExpression = _.flow.sync(
  Object.entries,
  _.map.sync(_.diverge.sync([
    _.flow.sync(_.get(0), toHash, x => `#${x}`),
    _.flow.sync(_.get(1), toHash, x => `:${x}`),
  ])),
  _.map.sync(_.join(' = ')),
  _.join(', '),
  x => `set ${x}`,
)

// {
//   filter: {
//     operator: 'and',
//     operations: [
//       { operator: 'eq', field: 'a', values: ['hey'] },
//       { operator: 'gte', field: 'c', values: [1] },
//       { operator: 'lt', field: 'c', values: [6] },
//       { operator: 'lte', field: 'd', values: [5] },
//       { operator: 'gt', field: 'd', values: [0] },
//       { operator: 'prefix', field: 'e', values: ['wazz'] },
//     ],
//   },
// }
// => """
//   #toHash('a') = :toHash('hey') AND
//   #toHash('c') >= :toHash(1) AND #toHash('c') < :toHash(6) AND
//   #toHash('d') > :toHash(0) AND #toHash('d') <= :toHash(5) AND
//   begins_with(#toHash('e'), :toHash('wazz'))
// """
const toDynamoKeyConditionExpression = _.flow.sync(
  _.switch.sync(
    _.not.sync(_.eq.sync(_.get('filter.operator'), 'and')),
    _.flow.sync(
      _.get('filter.operator'),
      x => `Unknown filter operator ${x}`,
      exception.MalformedQuery,
      _.throw,
    ),
    _.id,
  ),
  _.get('filter.operations'),
  _.map.sync(({ operator: op, field, values }) => {
    const v = values[0]
    if (op === 'eq') return `#${toHash(field)} = :${toHash(v)}`
    if (op === 'gte') return `#${toHash(field)} >= :${toHash(v)}`
    if (op === 'lte') return `#${toHash(field)} <= :${toHash(v)}`
    if (op === 'gt') return `#${toHash(field)} > :${toHash(v)}`
    if (op === 'lt') return `#${toHash(field)} < :${toHash(v)}`
    if (op === 'prefix') return `begins_with(#${toHash(field)}, :${toHash(v)})`
    throw exception.MalformedQuery(`Unknown filter operator ${op}`)
  }),
  _.join(' AND '),
)

// config {...} => client {...} => resource {
//   _id: '1',
//   _source: { a: 1 },
// } => status {...}
const update = config => client => _.flow(
  _.diverge([
    _.flow(_.pick('_id'), _.map(toDynamoAttributeValue)),
    _.flow(_.get('_source'), _.map(toDynamoAttributeValue)),
  ]),
  _.diverge({
    TableName: getTableName(config),
    Key: _.get(0),
    UpdateExpression: _.flow(_.get(1), toDynamoUpdateExpression),
    ExpressionAttributeNames: _.flow(
      _.get(1),
      Object.keys,
      _.map(_.diverge([
        _.flow(toHash, x => `#${x}`),
        _.id,
      ])),
      Object.fromEntries,
    ),
    ExpressionAttributeValues: _.flow(
      _.get(1),
      Object.values,
      _.map(_.diverge([
        _.flow(toHash, x => `:${x}`),
        _.id,
      ])),
      Object.fromEntries,
    ),
    ReturnValues: 'NONE',
  }),
  client.updateItem,
  () => ({ ok: true }),
)

// config {...} => client {...} => id {...} => status {...}
const del = config => client => _.flow(
  _.pick('_id'),
  _.map(toDynamoAttributeValue),
  _.diverge({ TableName: getTableName(config), Key: _.id }),
  client.deleteItem,
  () => ({ ok: true }),
)

/*
config {...} => client {...} => query {
  filter: $ => $.and(
    $.eq('a', 'hey'),
    $.lt('b', 6), $.lte('c', 5),
    $.gt('d', 0), $.gte('e', 1),
    $.prefix('f', 'wazz'),
    $.between('g', 1, 5),
  ),
  sort: $ => $.orderBy($.asc('b'), $.desc('c')),
  limit: 100,
  cursor: any|null,
} => query_result {
  result: [{ _id: '1', _source: {...}, [_score: 5] }],
  cursor: any|null,
  meta: {
    count: 100,
  },
}
*/
const query = config => {
  const availableIndexes = new Set(_.flow.sync(
    _.map.sync(_.flow.sync(
      _.get('key'),
      _.map.sync(_.get('name')),
      _.join('-'),
      _.concat.sync('-index'),
    )),
  )(_.get('schema.indexes', [])(config)))
  return client => _.flow(
    _.map(_.flow(_.toFn, queryLexer)),
    _.switch(
      _.flow(
        _.diverge([
          _.flow(_.get('filter.operations'), _.map(_.get('field'))),
          _.flow(_.get('sort.operations'), _.map(_.get('field'))),
        ]),
        ([filterKeys, sortKeys]) => _.any(
          _.not(_.member(filterKeys))
        )(sortKeys),
      ),
      _.flow(
        _.diverge(['filter must include sort keys', _.id]),
        _.spread(exception.MalformedQuery),
        _.throw,
      ),
      _.id,
    ),
    _.diverge({
      TableName: getTableName(config),
      IndexName: _.flow(
        _.get('filter.operations'),
        _.map(_.get('field')),
        _.diverge([
          _.flow(_.join('-'), _.concat('-index')),
          _.flow(_.reverse, _.join('-'), _.concat('-index')),
        ]),
        ([guess1, guess2]) => {
          if (availableIndexes.has(guess1)) return guess1
          if (availableIndexes.has(guess2)) return guess2
          throw exception.IndexNotFound(`${guess1} or ${guess2}`)
        },
      ),
      ExclusiveStartKey: _.switch(
        _.has('cursor'),
        _.flow(
          _.get('cursor'),
          JSON.parse,
          _.map(toDynamoAttributeValue),
        ),
        () => undefined,
      ),
      ScanIndexForward: _.flow(
        _.get(['sort', 'operations', 0, 'operator']),
        _.switch(
          _.eq('asc', _.id), true,
          _.eq('desc', _.id), false,
          _.flow(
            x => `Unknown sort operator ${x}`,
            exception.MalformedQuery,
            _.throw
          ),
        ),
      ),
      Limit: _.get('limit', 10000),
      KeyConditionExpression: toDynamoKeyConditionExpression,
      ExpressionAttributeNames: _.flow(
        _.get('filter.operations'),
        _.map(_.flow(
          _.get('field'),
          _.diverge([
            _.flow(toHash, x => `#${x}`),
            _.id,
          ])
        )),
        Object.fromEntries,
      ),
      ExpressionAttributeValues: _.flow(
        _.get('filter.operations'),
        _.map(_.get('values')),
        _.flatten,
        _.map(_.diverge([
          _.flow(toHash, x => `:${x}`),
          toDynamoAttributeValue,
        ])),
        Object.fromEntries,
      ),
    }),
    _.filter(_.exists),
    client.query,
    _.diverge({
      result: _.flow(
        _.get('Items'),
        _.map(_.diverge({
          _id: _.flow(_.get('_id'), fromDynamoAttributeValue),
          _source: _.flow(
            _.exclude('_id'),
            _.map(fromDynamoAttributeValue),
          ),
        })),
      ),
      cursor: _.switch(
        _.has('LastEvaluatedKey'),
        _.flow(
          _.get('LastEvaluatedKey'),
          _.map(fromDynamoAttributeValue),
          JSON.stringify,
        ),
        null,
      ),
      meta: _.diverge({
        count: _.get('Count'),
      }),
    }),
  )
}

/*
 * uri => connection {
 *   uri: string,
 *   prefix: string,
 *   client: object,
 *   ready: () => undefined,
 *   put: object => undefined,
 *   get: string => object,
 *   update: object => undefined,
 *   delete: string => undefined,
 *   query: object => object
 *   init: () => undefined,
 *   free: () => undefined,
 * }
 */
const connectorDynamo = _.flow(
  _.diverge([
    _.id,
    _.flow(
      _.diverge({
        apiVersion: '2012-08-10',
        endpoint: _.get('endpoint'),
        accessKeyId: _.get(['credentials', 0], 'id'),
        secretAccessKey: _.get(['credentials', 1], 'secret'),
        region: _.get('region', 'x-x-x'),
      }),
      x => new DynamoDB(x),
      _.promisifyAll,
    ),
  ]),
  _.diverge({
    uri: _.flow(_.get(0), uri.encodePublic),
    prefix: _.get([0, 'prefix']),
    client: _.get(1),
    ready: _.apply(ready),
    put: _.apply(put),
    get: _.apply(get),
    update: _.apply(update),
    delete: _.apply(del),
    query: _.apply(query),
    init: _.apply(init),
    free: _.apply(free),
  }),
)

connectorDynamo.toDynamoAttributeValue = toDynamoAttributeValue
connectorDynamo.fromDynamoAttributeValue = fromDynamoAttributeValue
connectorDynamo.toDynamoUpdateExpression = toDynamoUpdateExpression
connectorDynamo.toDynamoKeyConditionExpression = toDynamoKeyConditionExpression

module.exports = connectorDynamo
