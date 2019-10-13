const TableStore: any = require('tablestore')
const Long: any = TableStore.Long;
export class UpdateConfig {
    where?: Object = {};
    options?: {
        returning?: boolean
    } = {}
}
function fget(obj: any, key: string) {
    if (obj[key] instanceof Function) {
        return obj[key](obj);
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
    protected _parent: CTSYTableStore;
    constructor(cts: CTSYTableStore, TableName: string, define: Object) {
        this._parent = cts;
        this.define = define;
        this.table = TableName;
    }
    /**
     * 检查
     */
    protected async check() {
        if (this.checked) { return true; }
        if (!await this._parent.checkTable(this.table)) {
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
                    {
                        name: this.table + '_pk',
                        primaryKey: []
                    }
                ]
            }
            let auto = false;
            for (let key in this.define) {
                let c = this.define[key]
                if (key == '_id') {
                    // ctable.tableMeta.primaryKey.push({ name: "_id", type: "STRING" });
                    auto = true;
                    continue;
                }
                if (c.primaryKey) {
                    // if (ctable.tableMeta.primaryKey.length == 0) {
                    //     ctable.tableMeta.primaryKey.push({ name: "_id", type: "STRING" })
                    // }
                    let pkc: any = {
                        name: key,
                        type: getTSType(c.type),
                    }
                    if (c.autoIncrement && auto) {
                        // pkc.option = 'AUTO_INCREMENT'
                    }
                    ctable.tableMeta.primaryKey.push(pkc)
                    ctable.indexMetas[0].primaryKey.push(key);
                } else {
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
            await this._parent.instances.createTable(ctable);
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
        await this.check()
        let param = {
            tableName: this.table,
            indexName: this.table + '_pk',
            searchQuery: {
                limit: conf.limit || 10,
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
                getTotalCount: false
            },
            columnToGet: { //返回列设置：RETURN_SPECIFIED(自定义),RETURN_ALL(所有列),RETURN_NONE(不返回)
                returnType: TableStore.ColumnReturnType.RETURN_SPECIFIED,
                returnNames: conf.fields
            }
        }
        let queryTypes: any = {};
        for (let x in conf.where) {
            switch (x) {
                case 'between': break;
                case 'gt': break;
                case 'lt': break;
                case 'gte': break;
                case 'lte': break;
                default:
                    //精确查找
                    // queryTypes.push();
                    queryTypes[TableStore.QueryType.MATCH_ALL_QUERY] = {
                        fieldName: x,
                        term: conf.where[x]
                    }
                    break;
            }
        }
        let qts = Object.keys(queryTypes);
        if (qts.length == 1) {
            param.searchQuery.query.queryType = Number(qts[0]);
        }
        for (let x in queryTypes) {
            param.searchQuery.query.query = queryTypes[x];
        }
        let rs = await this._parent.instances.search(param), data = [];
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
        return data;
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
                    row.attributeColumns.push({ [x]: getLongFunc(obj.type, conf[x] !== undefined ? conf[x] : fget(obj, 'defaultValue')) })
                }
            }
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
    }
    /**
     * 查询并分页
     * @param conf 
     */
    async findAndCountAll(conf: any) {
        await this.check()
        debugger
    }
    /**
     * 删除数据
     * @param conf 
     */
    async destroy(conf: any) {
        await this.check()
        debugger
    }
    /**
     * 更新数据
     */
    async update(data: any, conf: any) {
        await this.check()
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
        if (this.existedTables.includes(TableName)) { return true; } return false;
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