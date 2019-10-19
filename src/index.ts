import * as Sequelize from 'sequelize';
const TableStore: any = require('tablestore')
const Long: any = TableStore.Long;
const Op: any = Sequelize.Op;
import * as _ from 'lodash'
export class UpdateConfig {
    where?: Object = {};
    options?: {
        returning?: boolean
    } = {}
}
async function fget(obj: any, key: string, o?: any) {
    if (obj[key] instanceof Function) {
        return await obj[key](obj, o);
    }
    return obj[key];
}
/**
 * 区域表
 */
export enum Regin {
    "cn-hangzhou" = "cn-hangzhou",
    "cn-hangzhou-finance" = "cn-hangzhou-finance",
    "cn-shanghai" = "cn-shanghai",
    "cn-shanghai-finance-1" = "cn-shanghai-finance-1",
    "cn-beijing" = "cn-beijing",
    "cn-zhangjiakou" = "cn-zhangjiakou",
    "cn-huhehaote" = "cn-huhehaote",
    "cn-shenzhen" = "cn-shenzhen",
    "cn-hongkong" = "cn-hongkong",
    "ap-southeast-1" = "ap-southeast-1",
    "us-east-1" = "us-east-1",
    "us-west-1" = "us-west-1",
    "ap-northeast-1" = "ap-northeast-1",
    "eu-central-1" = "eu-central-1",
    "me-east-1" = "me-east-1",
    "ap-southeast-2" = "ap-southeast-2",
    "ap-southeast-3" = "ap-southeast-3",
    "ap-southeast-5" = "ap-southeast-5",
    "ap-south-1" = "ap-south-1",
}

export enum TableStoreHosts {

}
export enum ErrorType {
    NOT_SUPPORT = "NotSupport"
}
enum DataTypeMap {
    'BIGINT'
}
export function getTSType(type: any) {
    let tn: string = ((type.name || type.key)).toLowerCase();
    if (tn.includes('int')) {
        return 'INTEGER';
    } else if (tn.includes('char')) {
        return 'STRING';
    } else if (['timestimp', 'datetime', 'date', 'time'].includes(tn)) {
        return 'INTEGER';
    }
    return "BINARY"
    debugger
}
export function getLongFunc(type: any, value: any) {
    let ts = getTSType(type);
    switch (ts) {
        case 'INTEGER':
            return Long.fromNumber(Number(value));
            break;
        case 'STRING':
            return value;
            break;
    }
    return value;
}
export function toLongFunc(type: any, value: any) {
    let ts = getTSType(type);
    switch (ts) {
        case 'INTEGER':
            return value.toNumber();
            break;
        case 'STRING':
            return value;
            break;
    }
    return value;
}
/**
 * 数据操作
 */
class ModelsDefine {
    checked: boolean = false;
    define: any
    table: string;
    primaryKeys: string[] = [];
    protected _parent: CTSYTableStore;
    constructor(cts: CTSYTableStore, TableName: string, define: any) {
        this._parent = cts;
        this.define = define;
        this.table = TableName;
        for (let x in define) {
            if (define[x].primaryKey) {
                this.primaryKeys.push(x);
            }
        }
    }
    /**
     * 检查
     */
    protected async check() {
        if (this.checked) { return true; }
        if (!await this._parent.checkSearchIndex(this.table + '_pk')) {
            //创建表
            let ctable: any = {
                tableMeta: {
                    tableName: this.table,
                    primaryKey: [

                    ],
                    definedColumn: []
                },
                reservedThroughput: {
                    capacityUnit: {
                        read: 0,
                        write: 0
                    }
                },
                tableOptions: {
                    timeToLive: -1,// 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
                    maxVersions: 1// 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).
                },
                indexMetas: [
                    // {
                    //     name: this.table + '_pk',
                    //     primaryKey: []
                    // }
                ]
            }
            let cindex: any = {
                tableName: this.table,
                indexName: this.table + '_pk',
                schema: {
                    fieldSchemas: [

                    ]
                },
                indexSetting: { //索引的配置选项
                    "routingFields": [], //仅支持将主键设为routingFields
                    "routingPartitionSize": null
                },
                // indexSort: {//不支持含含NESTED的索引，
                //     sorters: [
                //     ]
                // }
            };
            let auto = false;
            for (let key in this.define) {
                let c = this.define[key]
                if (key == '_id') {
                    // ctable.tableMeta.primaryKey.push({ name: "_id", type: "STRING" });
                    auto = true;
                    continue;
                }
                let pkc: any = {
                    name: key,
                    type: getTSType(c.type),
                }
                if (c.primaryKey) {
                    // if (ctable.tableMeta.primaryKey.length == 0) {
                    //     ctable.tableMeta.primaryKey.push({ name: "_id", type: "STRING" })
                    // }
                    if (c.autoIncrement && auto) {
                        // pkc.option = 'AUTO_INCREMENT'
                    }
                    // if (cindex.indexSetting.routingFields.length == 0) {
                    //     cindex.indexSetting.routingFields.push(key);
                    // }
                    ctable.tableMeta.primaryKey.push(pkc)
                    if (key == 'CTime') {
                        // cindex.indexSort.sorters.push(
                        //     {
                        //         fieldSort: {
                        //             fieldName: "CTime",
                        //             order: TableStore.SortOrder.SORT_ORDER_DESC //设置indexSort顺序
                        //         }
                        //     })
                    }
                    cindex.schema.fieldSchemas.push({
                        fieldName: key,
                        fieldType: getTSType(c.type) == 'INTEGER' ? TableStore.FieldType.LONG : TableStore.FieldType.KEYWORD,// 设置字段名、类型
                        index: true,// 设置开启索引
                        enableSortAndAgg: true,// 设置开启排序和统计功能
                        store: false,
                        isAnArray: false
                    })
                    cindex.indexSetting.routingFields.push(key)
                } else {
                    cindex.schema.fieldSchemas.push({
                        fieldName: key,
                        fieldType: getTSType(c.type) == 'INTEGER' ? TableStore.FieldType.LONG : TableStore.FieldType.KEYWORD,// 设置字段名、类型
                        index: true,// 设置开启索引
                        enableSortAndAgg: true,// 设置开启排序和统计功能
                        store: false,
                        isAnArray: false
                    })
                    // ctable.tableMeta.definedColumn.push({
                    //     name: key,
                    //     type: 1
                    // })
                }
            }
            // ctable.tableMeta.definedColumn.push({
            //     name: 'key',
            //     type: 1
            // })
            if (!await this._parent.checkTable(this.table)) {
                await this._parent.instances.createTable(ctable);
            }
            await this._parent.instances.createSearchIndex(cindex);
        }
        return this.checked = true
    }
    /**
     * 发起SQL查询
     * @param SQL 
     */
    query(SQL: string) {
        throw new Error(ErrorType.NOT_SUPPORT)
    }
    /**
     * 执行SQL
     * @param SQL 
     * @param Type 
     */
    async exec(SQL: string, Type: string) {
        throw new Error(ErrorType.NOT_SUPPORT)
    }
    /**
     * 查询全部
     * @param conf 
     */
    async findAll(conf: any) {
        conf.count = false;
        return (await this.findAndCountAll(conf)).rows;
    }
    /**
     * 创建数据
     * @param conf 
     */
    async create(conf: any) {
        await this.bulkCreate([conf])
        return conf;
    }
    /**
     * 构建数据
     * @param data 
     */
    async build(data: Object) {
        await this.check()
        debugger
    }
    /**
     * 批量创建
     * @param data 
     */
    async bulkCreate(data: any[]) {
        await this.check()
        let params: { [index: string]: any } = {
            tables: [
                {
                    tableName: this.table,
                    rows: []
                }
            ]
        };
        for (let conf of data) {
            let row: { type: string, condition: any, primaryKey: { [index: string]: any }, attributeColumns: { [index: string]: any }, [index: string]: any } = {
                type: "PUT",
                condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
                primaryKey: [],
                attributeColumns: [],
                returnContent: { returnType: TableStore.ReturnType.Primarykey }
            }
            for (let x in this.define) {
                let obj = this.define[x];
                if (obj.primaryKey) {
                    if (obj.autoIncrement) {
                        // row.primaryKey.push({ [x]: getLongFunc(obj.type, conf[x] || 0) })
                    } else {
                        row.primaryKey.push({
                            [x]: getLongFunc(obj.type, conf[x] || '')
                        })
                    }
                } else {
                    row.attributeColumns.push({ [x]: getLongFunc(obj.type, conf[x] !== undefined ? conf[x] : await fget(obj, 'defaultValue', this)) })
                }
            }
            params.tables[0].rows.push(row);
        }
        let rs: any = { tables: [] }
        if (params.tables[0].rows.length > 200) {
            let ps = [];
            for (let i = 0; i < data.length; i += 200) {
                let pt: { [index: string]: any } = {
                    tables: [
                        {
                            tableName: this.table,
                            rows: params.tables[0].rows.slice(i, i + 200)
                        }
                    ]
                };
                ps.push(this._parent.instances.batchWriteRow(pt))
            }
            rs = {
                tables: [...(await Promise.all(ps)).map((v) => {
                    return v.tables;
                })]
            };
        } else {
            rs = await this._parent.instances.batchWriteRow(params)
        }
        //TODO 检测是否成功
        let pass = true;
        for (let x of rs.tables) {
            if (!x.isOk) {
                pass = false;
                throw new Error(x.errorCode)
            }
        }
        return data;
    }
    /**
     * 查询并分页
     * @param conf 
     */
    async findAndCountAll(conf: any) {
        await this.check()
        let param: any = {
            tableName: this.table,
            indexName: this.table + '_pk',
            searchQuery: {
                limit: conf.limit && conf.limit <= 100 ? conf.limit : 100,
                offset: conf.offset || 0,
                query: {
                    queryType: TableStore.QueryType.MATCH_ALL_QUERY,
                    query: {}
                    // query: {
                    //     mustQueryies: [],
                    //     // shouldQueries: [],
                    //     // mustNotQueries: [],
                    // },
                    // minimumShouldMatch: 0
                },
                getTotalCount: conf.count || true
            },
            columnToGet: { //返回列设置：RETURN_SPECIFIED(自定义),RETURN_ALL(所有列),RETURN_NONE(不返回)
                returnType: TableStore.ColumnReturnType.RETURN_SPECIFIED,
                returnNames: conf.fields || conf.attributes
            }
        }
        let queryTypes: any = {}, where = false;
        for (let x in conf.where) {
            where = true;
            let type = -1, query = {}, val = conf.where[x], key = x;
            if ('object' == typeof conf.where[x]) {
                key = x;
                x = Object.keys(conf.where[x])[0]
                val = conf.where[key][x]
            }
            switch (x) {
                case 'between':
                    //精确查找
                    if (val instanceof Array) {
                        type = TableStore.QueryType.RANGE_QUERY;
                        query = {
                            fieldName: key,
                            rangeFrom: val[0],
                            rangeTo: val[1],
                            includeLower: true,
                            includeUpper: true,
                        }
                    } else {
                        throw new Error('between Search Must Array')
                    }
                    break;
                case 'or':

                    break;
                case 'gt':
                case 'gte':
                    if ("number" == typeof val) {
                        type = TableStore.QueryType.RANGE_QUERY;
                        query = {
                            fieldName: key,
                            rangeFrom: val,
                            // rangeTo: conf.where[x][1],
                            // includeLower: true,
                            includeLower: x == 'gte',
                        }
                    } else {
                        throw new Error('gt/lt/gte/lte Search Must number')
                    }
                    break;
                case 'lt':
                case 'lte':
                    if ("number" == typeof val) {
                        type = TableStore.QueryType.RANGE_QUERY;
                        query = {
                            fieldName: key,
                            // rangeFrom: conf.where[x][0],
                            rangeTo: val,
                            // includeLower: true,
                            includeUpper: x == 'lte',
                        }
                    } else {
                        throw new Error('gt/lt/gte/lte Search Must number')
                    }
                    break;
                case 'in':
                    //精确查找
                    if (val instanceof Array) {
                        type = TableStore.QueryType.TERMS_QUERY;
                        query = {
                            fieldName: key,
                            terms: val
                        }
                    } else {
                        throw new Error('In Search Must Array')
                    }
                    break;
                case 'like':
                    let value = val.replace(/%/g, '*');
                    if (value.indexOf('*') == 0) {
                        value.replace('*', '?');
                    }
                    type = TableStore.QueryType.WILDCARD_QUERY;
                    query = {
                        fieldName: key,
                        value
                    }
                    break;
                default:
                    //精确查找
                    type = TableStore.QueryType.TERM_QUERY;
                    query = {
                        fieldName: key,
                        term: val
                    }
                    break;
            }
            if (type > 0) {
                if (!queryTypes[type]) {
                    queryTypes[type] = [];
                }
                queryTypes[type].push(query)
            }
        }
        if (where) {
            param.searchQuery.query.queryType = TableStore.QueryType.BOOL_QUERY;
            param.searchQuery.query.query = {
                mustQueries: []
            }
            for (let x in queryTypes) {
                for (let o of queryTypes[x]) {
                    param.searchQuery.query.query.mustQueries.push({
                        queryType: Number(x),
                        query: o
                    });
                }
            }
        }
        // }
        let rs = await this._parent.instances.search(param), data = [];
        if (rs.nextToken && (!conf.limit || conf.limit > param.searchQuery.limit)) {
            while (rs.nextToken && rs.nextToken.length > 0) {
                param.searchQuery.token = rs.nextToken;
                let ors = await this._parent.instances.search(param)
                rs.rows.push(...ors.rows);
                rs.nextToken = ors.nextToken;
            }
        }
        for (let row of rs.rows) {
            let d: any = {};
            for (let r of row.primaryKey) {
                d[r.name] = r.value;
            }
            if (row.attributes.length > 0) {
                d.timestamp = row.attributes[0].timestamp.toNumber()
                for (let r of row.attributes) {
                    d[r.columnName] = toLongFunc(this.define[r.columnName].type, r.columnValue);
                }
            }
            data.push(d)
        }
        return {
            count: Number(rs.totalCounts),
            rows: data,
        };
    }
    /**
     * 删除数据
     * @param conf 
     */
    async destroy(confs: any) {
        await this.check()
        let params: { [index: string]: any } = {
            tables: [
                {
                    tableName: this.table,
                    rows: []
                }
            ]
        };
        let row: { type: string, condition: any, primaryKey: { [index: string]: any }, attributeColumns: { [index: string]: any }, [index: string]: any } = {
            type: "DELETE",
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
            primaryKey: [],
            attributeColumns: [],
            returnContent: { returnType: TableStore.ReturnType.Primarykey }
        }
        /**
         * 是否需要前置查询
         */
        let needFindFirst = false;
        for (let key in confs) {
            switch (key) {
                case 'where':
                    for (let x in confs[key]) {
                        if (!['number', 'boolean', 'string'].includes(typeof confs[key][x])) {
                            needFindFirst = true;
                            break;
                        }
                        if (this.define[x].primaryKey) {
                            row.primaryKey.push({
                                [x]: getLongFunc(this.define[x].type, confs[key][x])
                            })
                        } else {
                            //需要查询出主键再操作
                            needFindFirst = true;
                            break;
                        }
                    }
                    break;
            }
        }
        if (Object.keys(row.primaryKey).length != this.primaryKeys.length) {
            needFindFirst = true;
        }
        if (needFindFirst) {
            let primaryKeys = await this.findAndCountAll(Object.assign(confs, { fields: this.primaryKeys }));
            if (primaryKeys.count == 0) { return true; }
            for (let pks of primaryKeys.rows) {
                let rowt: { type: string, condition: any, primaryKey: { [index: string]: any }, attributeColumns: { [index: string]: any }, [index: string]: any } = {
                    type: "DELETE",
                    condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
                    primaryKey: Object.keys(pks).map((v: string) => {
                        return { [v]: pks[v] }
                    }),
                    attributeColumns: row.attributeColumns,
                    returnContent: { returnType: TableStore.ReturnType.Primarykey }
                }
                params.tables[0].rows.push(rowt);
            }
        } else {
            params.tables[0].rows.push(row);
        }
        if (params.tables[0].rows.length == 0) {
            return true;
        }
        let rs = await this._parent.instances.batchWriteRow(params)
        //TODO 检测是否成功
        let pass = true;
        for (let x of rs.tables) {
            if (!x.isOk) {
                pass = false;
                throw new Error(x.errorCode)
            }
        }
        return pass;
    }
    /**
     * 更新数据
     */
    async update(data: any, confs: any) {
        await this.check()
        let params: { [index: string]: any } = {
            tables: [
                {
                    tableName: this.table,
                    rows: []
                }
            ]
        };
        let row: { type: string, condition: any, primaryKey: { [index: string]: any }, attributeColumns: { [index: string]: any }, [index: string]: any } = {
            type: "UPDATE",
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
            primaryKey: [],
            attributeColumns: [{ PUT: [] }],
            returnContent: { returnType: TableStore.ReturnType.Primarykey }
        }
        for (let x in data) {
            row.attributeColumns[0].PUT.push({ [x]: getLongFunc(this.define[x].type, data[x]) })
        }
        /**
         * 是否需要前置查询
         */
        let needFindFirst = false;
        for (let key in confs) {
            switch (key) {
                case 'where':
                    for (let x in confs[key]) {
                        if (!['number', 'boolean', 'string'].includes(typeof confs[key][x])) {
                            needFindFirst = true;
                            break;
                        }
                        if (this.define[x].primaryKey) {
                            row.primaryKey.push({
                                [x]: getLongFunc(this.define[x].type, confs[key][x])
                            })
                        } else {
                            //需要查询出主键再操作
                            needFindFirst = true;
                            break;
                        }
                    }
                    break;
            }
        }
        if (Object.keys(row.primaryKey).length != this.primaryKeys.length) {
            needFindFirst = true;
        }
        if (needFindFirst) {
            let primaryKeys = await this.findAll(Object.assign(confs, { fields: this.primaryKeys }));
            for (let pks of primaryKeys) {
                let rowt: { type: string, condition: any, primaryKey: { [index: string]: any }, attributeColumns: { [index: string]: any }, [index: string]: any } = {
                    type: "UPDATE",
                    condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
                    primaryKey: Object.keys(pks).map((v: string) => {
                        return { [v]: pks[v] }
                    }),
                    attributeColumns: row.attributeColumns,
                    returnContent: { returnType: TableStore.ReturnType.Primarykey }
                }
                params.tables[0].rows.push(rowt);
            }
        } else {
            params.tables[0].rows.push(row);
        }
        let rs = await this._parent.instances.batchWriteRow(params)
        //TODO 检测是否成功
        let pass = true;
        for (let x of rs.tables) {
            if (!x.isOk) {
                pass = false;
                throw new Error(x.errorCode)
            }
        }
        return data;
        debugger
    }
}
/**
 * 表格存储
 */
export default class CTSYTableStore {
    models: { [index: string]: ModelsDefine } = {};
    instances: any;
    existedTables: string[] = [];
    existedSearchIndexs: string[] = [];
    constructor(database: string, username: string, password: string, options: any) {
        this.instances = new TableStore.Client({
            accessKeyId: username,
            accessKeySecret: password,
            endpoint: options.host,
            instancename: database,
            maxRetries: 20,//默认20次重试，可以省略这个参数。
        });
    }
    /**
     * 检查表存在性
     * @param TableName 
     */
    async checkTable(TableName: string) {
        if (this.existedTables.includes(TableName)) { return true; }
        if (this.existedTables.length == 0) {
            this.existedTables = (await this.instances.listTable()).tableNames;
        }
        return this.existedTables.includes(TableName)
    }
    async checkSearchIndex(IndexName: string) {
        if (this.existedSearchIndexs.includes(IndexName)) { return true; }
        if (this.existedSearchIndexs.length == 0) {
            this.existedSearchIndexs = (await this.instances.listSearchIndex()).indices.map((v: any) => v.indexName);
        }
        return this.existedSearchIndexs.includes(IndexName)
    }
    async createTable(Name: string, ) { }
    define(TableName: string, DbDefine: any) {
        return this.models[TableName] = new ModelsDefine(this, TableName, DbDefine)
    }

    /**
     * 获取事物
     */
    async transaction(): Promise<Transaction> {
        let t = new Transaction()
        return t;
    }
}
export class Transaction {
    LOCK: boolean = false;
    commit() { }
    rollback() { }
}