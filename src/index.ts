import * as sqlite from 'sqlite3';
import * as Bluebird from 'bluebird';
import * as uuid from 'uuid';
import { Stream } from 'stream';
import { Semaphore } from 'prex';

export declare class Statement extends sqlite.Statement {
  public bindAsync(params: any): Promise<void>;

  public eachAsync(params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(callback?: (err: Error, row: any) => void): Promise<number>;

  public resetAsync(): Promise<void>;

  public finalizeAsync(): Promise<void>;

  public runAsync(params?: any): Promise<Statement>;

  public getAsync(params?: any): Promise<any>;

}

export declare class Database extends sqlite.Database {
  public closeAsync():Promise<void>;

  public execAsync(sql: string): Promise<void>;

  public runAsync(sql: string): Promise<sqlite.Statement>;
  public runAsync(sql: string, ...params: any[]): Promise<sqlite.Statement>;
  public runAsync(sql: string, params: any): Promise<sqlite.Statement>;

  public allAsync(sql: string): Promise<any[]>;
  public allAsync(sql: string, ...params: any[]): Promise<any[]>;
  public allAsync(sql: string, params: any): Promise<any[]>;

  public getAsync(sql: string, params: any): Promise<any>;
  public getAsync(sql: string, ...params: any[]): Promise<any>;
  public getAsync(sql: string): Promise<any>;

  public eachAsync(sql: string, params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(sql: string, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(...params: any[]): Promise<number>;

  public prepareAsync(sql: string, params: any): Promise<Statement>;
  public prepareAsync(sql: string, ...params: any[]): Promise<Statement>;
  public prepareAsync(sql: string): Promise<Statement>;

  public eachStream(sql: string, ...params: any[]): Stream;

  public execAsync(t: Transaction, sql: string): Promise<void>;

  public beginTransaction(options?: TransactionOptions): Promise<Transaction>;
}

export type TransactionOptions = {type: "IMMEDIATE" | "DEFERRED" | "EXCLUSIVE" };

function transactionise(semaphore: Semaphore, obj: any, names: string[]) {
  for (let name of names) {
    const oldF = obj[name + "Async"].bind(obj);
    obj[name + "Async"] = async function(...params: any[]) {
      if (params[0] instanceof Transaction) {
        const t:Transaction = params.shift();
        if (!t.isOpen) {
          throw new Error("Transaction is closed");
        }
        if (name == "run" && params.length == 1) {
          params.push([]);
        }
        return oldF(...params);
      } else {
        await semaphore.wait();
          return oldF(...params)
            .then((x:any) => {semaphore.release(); return x;},
                  (error:any) => {semaphore.release(); throw error});
        }
      }
    }
  }

export class Transaction {
  externalSemaphore: Semaphore;
  internalSemaphore: Semaphore;
  options?: TransactionOptions;
  isOpen: boolean;
  database: Database;
  savepoint?: string;

  constructor(database: Database, semaphore: Semaphore, options?: TransactionOptions) {
    this.database = database;
    this.externalSemaphore = semaphore;
    this.internalSemaphore = new Semaphore(1);
    this.isOpen = false;
    this.options = options;
  }

  static begin(database: Database, semaphore: Semaphore, options?: TransactionOptions): Promise<Transaction> {
    const t = new Transaction(database, semaphore, options);
    return t.begin();
  }

  private async begin(): Promise<Transaction> {
    if (this.isOpen) {
      throw new Error("Transaction already begun");
    }
    await this.externalSemaphore.wait();
    this.isOpen = true;
    this.savepoint = uuid.v4();
    if (this.options) {
      await this.database.execAsync(this, `BEGIN ${this.options.type} TRANSACTION`);
    } else {
      await this.database.execAsync(this, `SAVEPOINT '${this.savepoint}'`);
    }
    return this;
  }

  async beginNew(): Promise<Transaction> {
    const internalTransaction = new Transaction(this.database, this.internalSemaphore);
    return internalTransaction.begin();
  } 

  async commit() {
    if (this.options) {
      await this.database.execAsync(this, `COMMIT`);
    } else {
      await this.database.execAsync(this, `RELEASE '${this.savepoint}'`);
    }
    await this.externalSemaphore.release();
  }

  async rollback() {
    if (this.options) {
      await this.database.execAsync(this, `ROLLBACK`);
    } else {
      await this.database.execAsync(this, `ROLLBACK TO '${this.savepoint}'`);
    }
    await this.externalSemaphore.release();
  }

  public async execAsync(sql: string): Promise<void> {
    await this.internalSemaphore.wait();
    try {
      return this.database.execAsync(this, sql);
    } finally {
      await this.internalSemaphore.release();
    }
  }

  public runAsync(sql: string, params: any): Promise<sqlite.Statement>;
  public runAsync(sql: string, ...params: any[]): Promise<sqlite.Statement>;
  public async runAsync(sql: string): Promise<sqlite.Statement> {
    await this.internalSemaphore.wait();
    try {
      const args = [this].concat(Array.from(arguments));
      return this.database.runAsync.apply(this.database, args);
    } finally {
      await this.internalSemaphore.release();
    }
  }

  public allAsync(sql: string, ...params: any[]): Promise<any[]>;
  public allAsync(sql: string, params: any): Promise<any[]>;
  public async allAsync(sql: string): Promise<any[]> {
    await this.internalSemaphore.wait();
    try {
      const args = [this].concat(Array.from(arguments));
      return this.database.allAsync.apply(this.database, args);
    } finally {
      await this.internalSemaphore.release();
    }
  }

  public getAsync(sql: string, params: any): Promise<any>;
  public getAsync(sql: string, ...params: any[]): Promise<any>;
  public async getAsync(sql: string): Promise<any> {
    await this.internalSemaphore.wait();
    try {
      const args = [this].concat(Array.from(arguments));
      return this.database.getAsync.apply(this.database, args);
    } finally {
      await this.internalSemaphore.release();
    }
  }


  public eachAsync(sql: string, params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(sql: string, callback: (err: Error, row: any) => void): Promise<number>;
  public async eachAsync(...params: any[]): Promise<number> {
    await this.internalSemaphore.wait();
    try {
      const args = [this].concat(Array.from(arguments));
      return this.database.eachAsync.apply(this.database, args);
    } finally {
      await this.internalSemaphore.release();
    }
  }

}

export namespace SQLite{

  export async function open(path: string, mode: number = sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE): Promise<Database> {
    return new Promise<Database>( (resolve, reject) => {
      const db = new sqlite.Database(path, mode, (error) => {
        if (error) { reject(error); }
        else { resolve(promisify(db, true)); }
      });
    });
  }

  function promisify(database: sqlite.Database, shouldSerialize:boolean): Database {
    const semaphore = new Semaphore(1);
    const obj = Bluebird.promisifyAll(database) as any;
    if (shouldSerialize) {

      obj.rollback = (t: Transaction) => {return t.rollback()};
      obj.commit = (t: Transaction) => {return t.commit()};
      obj.beginTransaction = (options?: TransactionOptions) => {
        return Transaction.begin(obj, semaphore, options);
      }

      transactionise(semaphore, obj, ["run", "close", "exec", "each", "all", "get"]);
    }

    obj.eachStream = function (sql: string, ...params: any[]): Stream {
      const pipe = new Stream.PassThrough({objectMode: true});
      obj.eachAsync(sql, params, (err: Error, row: any) => {
        pipe.push(row);
      }).then(() => {
        pipe.push(null);
      });
      return pipe;
    }

    obj.prepareAsync = function(...args:any[]): Promise<Statement> {
      let stmt: sqlite.Statement;

      const promise = new Promise<Statement>( (resolve, reject) => {
        const callback = (err: Error) => {
          if (err) { reject(err); }
          else { resolve(); }
        };
        const allArgs = args.concat(callback);
        stmt = obj.prepare.apply(obj, allArgs);
      });

      return promise.then( () => { 
        const pStmt = Bluebird.promisifyAll(stmt) as Statement 
        if (shouldSerialize) {
          transactionise(semaphore, pStmt, ["each", "run", "get", "finalize", "reset"]);
        }
        return pStmt;
      });
    }
    return obj;
  }
}
