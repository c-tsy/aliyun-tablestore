const TableStore: any = require('tablestore')
export class UpdateConfig {
    where?: Object = {};
    options?: {
        returning?: boolean
    } = {}
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
    let tn: string = (type.name || type.key).toLowerCase();
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
                }
            }
            for (let key in this.define) {
                let c = this.define[key]
                if (c.primaryKey) {
                    if (ctable.tableMeta.primaryKey.length == 0) {
                        ctable.tableMeta.primaryKey.push({ name: "_id", type: "STRING" })
                    }
                    let pkc: any = {
                        name: key,
                        type: getTSType(c.type),
                    }
                    if (c.autoIncrement) {
                        pkc.option = 'AUTO_INCREMENT'
                    }
                    ctable.tableMeta.primaryKey.push(pkc)
                } else {
                    ctable.tableMeta.definedColumn.push({
                        name: key,
                        type: 1
                    })
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
        let rs = await this._parent.instances.batchGetRow({
            tableName: this.table,
            searchQuery: {
                offset: 0,
                limit: 10, //如果只为了取行数，但不需要具体数据，可以设置limit=0，即不返回任意一行数据。
                query: { // 设置查询类型为TermQuery
                    queryType: TableStore.QueryType.TERM_QUERY,
                    query: {
                        fieldName: "EID",
                        term: "1"
                    }
                },
                getTotalCount: true // 结果中的TotalCount可以表示表中数据的总行数， 默认false不返回
            },
            columnToGet: { //返回列设置：RETURN_SPECIFIED(自定义),RETURN_ALL(所有列),RETURN_NONE(不返回)
                returnType: conf.attributes && conf.attributes.length > 0 ? TableStore.ColumnReturnType.RETURN_SPECIFIED : TableStore.ColumnReturnType.RETURN_ALL,
                returnNames: conf.attributes || []
            }
        })
        return [{ dataValues: 1 }];
    }
    /**
     * 创建数据
     * @param conf 
     */
    async create(conf: any) {
        await this.check()
        debugger
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
    async bulkCreate(data: Object) {
        await this.check()
        debugger
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