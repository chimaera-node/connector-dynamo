const _ = require('rubico')
const a = require('a-sert')
const exception = require('@chimaera/exception')
const connectorDynamo = require('.')

describe('connectorDynamo', () => {
  it('establishes a DynamoDB connection', () => _.flow(
    connectorDynamo,
    a.eq('chimaera://[dynamo::local]/my_table', _.get('uri')),
    a.eq('my_table', _.get('prefix')),
    a.eq('localhost', _.get('client.endpoint.hostname')),
    a.eq('http:', _.get('client.endpoint.protocol')),
    a.eq(8000, _.get('client.endpoint.port')),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({
      _id: '1',
      _source: { a: 1, b: 'yo', c: [{ d: true }] },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: 1, b: 'yo', c: [{ d: true }] },
    })({ _id: '1' })),
    _.effect(conn => conn.update({
      _id: '1',
      _source: { a: 2, c: { d: true } },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: 2, b: 'yo', c: { d: true } },
    })({ _id: '1' })),
    _.effect(conn => conn.put({ _id: '1', _source: {} })),
    _.effect(conn => a.eq(conn.get, { _id: '1', _source: {} })({ _id: '1' })),
    _.effect(conn => conn.delete({ _id: '1' })),
    _.effect(conn => a.err(conn.get, exception.ResourceNotFound(`${conn.uri}/1`))({ _id: '1' })),
    _.effect(conn => conn.free()),
  )({
    connector: 'dynamo',
    env: 'local',
    prefix: 'my_table',
    endpoint: 'http://localhost:8000',
    schema: {
      base: {
        dynamo_capacity: 'provisioned',
        dynamo_rcu: 5, dynamo_wcu: 5,
      },
    },
  })).timeout(2 * 60 * 1000)

  it('queries', () => _.flow(
    connectorDynamo,
    _.effect(conn => conn.free()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.put({ _id: '1', _source: { a: 'yo', b: 1000 } })),
    _.effect(conn => conn.put({ _id: '2', _source: { a: 'yo', b: 1001 } })),
    _.effect(conn => conn.put({ _id: '3', _source: { a: 'yo', b: 1002 } })),
    _.effect(conn => conn.put({ _id: '4', _source: { a: 'yo', b: 1003 } })),
    _.effect(conn => conn.put({ _id: '5', _source: { a: 'yo', b: 1004 } })),

    _.effect(conn => _.flow(
      conn.query,
      a.eq(_.get('result'), [
        { _id: '1', _source: { a: 'yo', b: 1000 } },
        { _id: '2', _source: { a: 'yo', b: 1001 } },
        { _id: '3', _source: { a: 'yo', b: 1002 } },
      ]),
      a.eq(_.get('cursor'), '{"a":"yo","b":1002,"_id":"3"}'),
      a.eq(_.get('meta.count'), 3),
    )({
      filter: $ => $.and($.eq('a', 'yo'), $.gte('b', 1000)),
      sort: $ => $.orderBy($.asc('b')),
      limit: 3,
    })),

    _.effect(conn => _.flow(
      conn.query,
      a.eq(_.get('result'), [
        { _id: '4', _source: { a: 'yo', b: 1003 } },
        { _id: '5', _source: { a: 'yo', b: 1004 } },
      ]),
      a.ok(_.flow(_.get('cursor'), _.dne)),
      a.eq(_.get('meta.count'), 2),
    )({
      filter: $ => $.and($.eq('a', 'yo'), $.gte('b', 1000)),
      sort: $ => $.orderBy($.asc('b')),
      limit: 3,
      cursor: '{"a":"yo","b":1002,"_id":"3"}',
    })),

    _.effect(conn => _.flow(
      conn.query,
      a.eq(_.get('result'), [
        { _id: '5', _source: { a: 'yo', b: 1004 } },
        { _id: '4', _source: { a: 'yo', b: 1003 } },
        { _id: '3', _source: { a: 'yo', b: 1002 } },
      ]),
      a.eq(_.get('cursor'), '{"a":"yo","b":1002,"_id":"3"}'),
      a.eq(_.get('meta.count'), 3),
    )({
      filter: $ => $.and($.eq('a', 'yo'), $.gte('b', 1000)),
      sort: $ => $.orderBy($.desc('b')),
      limit: 3,
    })),

    _.effect(conn => _.flow(
      a.err(
        conn.query,
        exception.MalformedQuery('filter must include sort keys'),
      ),
    )({
      filter: $ => $.and($.eq('a', 'yo'), $.gte('b', 1000)),
      sort: $ => $.orderBy($.desc('c')),
      limit: 3,
    })),

    _.effect(conn => _.flow(
      a.err(
        conn.query,
        exception.IndexNotFound('c-d-index or d-c-index'),
      ),
    )({
      filter: $ => $.and($.eq('c', 'yo'), $.gte('d', 1000)),
      sort: $ => $.orderBy($.desc('d')),
      limit: 3,
    })),

    _.effect(conn => _.flow(
      a.err(
        conn.query,
        exception.MalformedQuery('Unknown sort operator exists'),
      ),
    )({
      filter: $ => $.and($.eq('a', 'yo'), $.gte('b', 1000)),
      sort: $ => $.orderBy($.exists('b')),
      limit: 3,
    })),

    _.effect(conn => conn.free()),
  )({
    connector: 'dynamo',
    env: 'local',
    prefix: 'my_table',
    endpoint: 'http://localhost:8000',
    index: [{ fields: ['a', 'b'] }],
    field_datatypes: { a: 'string', b: 'number' },
    dynamo_capacity: 'provisioned',
    dynamo_rcu: 5, dynamo_wcu: 5,
    schema: {
      base: {
        // dynamo_capacity: 'on_demand',
        dynamo_capacity: 'provisioned',
        dynamo_rcu: 5, dynamo_wcu: 5,
      },
      indexes: [
        {
          key: [
            { name: 'a', type: 'string' },
            { name: 'b', type: 'number' },
          ],
          dynamo_rcu: 5, dynamo_wcu: 5,
        },
      ],
    },
  })).timeout(2 * 60 * 1000)

  describe('toDynamoAttributeValue, fromDynamoAttributeValue', () => {
    it('string => { S: string }', () => _.flow(
      connectorDynamo.toDynamoAttributeValue,
      a.eq({ S: 'hey' }, _.id),
      connectorDynamo.fromDynamoAttributeValue,
      a.eq('hey', _.id),
    )('hey'))

    it('number => { N: "number" }', () => _.flow(
      connectorDynamo.toDynamoAttributeValue,
      a.eq({ N: '1' }, _.id),
      connectorDynamo.fromDynamoAttributeValue,
      a.eq(1, _.id),
    )(1))

    it('boolean => { BOOL: boolean }', () => _.flow(
      connectorDynamo.toDynamoAttributeValue,
      a.eq({ BOOL: true }, _.id),
      connectorDynamo.fromDynamoAttributeValue,
      a.eq(true, _.id),
    )(true))

    it('array => { L: [AttributeValue] }', () => _.flow(
      connectorDynamo.toDynamoAttributeValue,
      a.eq({
        L: [
          { S: 'a' },
          { S: 'b' },
          { N: '1' },
          { L: [{ BOOL: true }] },
        ],
      }, _.id),
      connectorDynamo.fromDynamoAttributeValue,
      a.eq(['a', 'b', 1, [true]], _.id),
    )(['a', 'b', 1, [true]]))

    it('object => { M: { k: AttributeValue } }', () => _.flow(
      connectorDynamo.toDynamoAttributeValue,
      a.eq({
        M: {
          a: { N: '1' },
          b: { S: 'yo' },
          c: { M: { d: { BOOL: true } } },
        },
      }, _.id),
      connectorDynamo.fromDynamoAttributeValue,
      a.eq({ a: 1, b: 'yo', c: { d: true } }, _.id),
    )({ a: 1, b: 'yo', c: { d: true } }))

    it('toDynamoAttributeValue throws TypeError on undefined', () => _.flow(
      a.err(
        connectorDynamo.toDynamoAttributeValue,
        new TypeError('cannot coerce undefined to dynamo attribute value'),
      ),
    )(undefined))

    it('fromDynamoAttributeValue throws TypeError on undefined', () => _.flow(
      a.err(
        connectorDynamo.fromDynamoAttributeValue,
        new TypeError('cannot extract from dynamo attribute value undefined'),
      ),
    )(undefined))

    it('fromDynamoAttributeValue throws TypeError on unrecognized key', () => _.flow(
      a.err(
        connectorDynamo.fromDynamoAttributeValue,
        new TypeError('cannot extract from dynamo attribute value { hey: 1 }'),
      ),
    )({ hey: 1 }))
  })

  describe('toDynamoUpdateExpression', () => {
    it('transforms an object to a dynamo update expression', () => _.flow(
      a.eq(
        connectorDynamo.toDynamoUpdateExpression,
        'set #d749929fe9 = :d4735e3a26, #0e09991f37 = :5cfba69805',
      ),
    )({ a: 2, c: { d: true } }))
  })

  describe('toDynamoKeyConditionExpression', () => {
    it('transforms a lexed query object to a dynamo key condition expression', () => _.flow(
      a.eq(
        connectorDynamo.toDynamoKeyConditionExpression,
        [
          '#d749929fe9 = :535047d9e2',
          '#0e09991f37 >= :6b86b273ff',
          '#0e09991f37 < :e7f6c01177',
          '#8f5fb86804 > :5feceb66ff',
          '#8f5fb86804 <= :ef2d127de3',
          'begins_with(#01b5a1c655, :114a7d600f)',
        ].join(' AND '),
      ),
    )({
      filter: {
        operator: 'and',
        operations: [
          { operator: 'eq', field: 'a', values: ['hey'] },
          { operator: 'gte', field: 'c', values: [1] },
          { operator: 'lt', field: 'c', values: [6] },
          { operator: 'gt', field: 'd', values: [0] },
          { operator: 'lte', field: 'd', values: [5] },
          { operator: 'prefix', field: 'e', values: ['wazz'] },
        ],
      },
    }))

    it('throws MalformedQueryException', () => _.flow(
      a.err(
        connectorDynamo.toDynamoKeyConditionExpression,
        exception.MalformedQuery('Unknown filter operator or'),
      ),
    )({
      filter: {
        operator: 'or',
        operations: [
          { operator: 'eq', field: 'a', values: ['hey'] },
          { operator: 'gte', field: 'c', values: [1] },
          { operator: 'lt', field: 'c', values: [6] },
          { operator: 'gt', field: 'd', values: [0] },
          { operator: 'lte', field: 'd', values: [5] },
          { operator: 'prefix', field: 'e', values: ['wazz'] },
        ],
      },
    }))
  })

  xit('playing', () => _.flow(
    connectorDynamo,
    _.get('client'),
    /*
    x => _.flow(
      _.diverge({ TableName: 'hey' }),
      _.tryCatch(
        x.describeTable,
        console.error,
      ),
    )(),
    */
    x => _.flow(
      x.listTables,
      _.get('TableNames'),
      _.map(_.flow(
        _.diverge({ TableName: _.id }),
        x.describeTable,
        _.trace,
        _.diverge([
          _.get('Table.TableName'),
          _.get('Table.KeySchema'),
          _.get('Table.AttributeDefinitions'),
          _.get('Table.GlobalSecondaryIndexes'),
        ]),
        _.trace,
      )),
    )(),
  )({
    credentials: {
      access_key_id: 'AKIAI5XSBO75NVEDKVFQ',
      secret_access_key: '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',
    },
    region: 'us-east-1',
  }))
})
