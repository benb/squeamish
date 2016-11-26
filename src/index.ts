import * as sqlite from 'sqlite3';
import * as Bluebird from 'bluebird';
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

  public eachAsync(t: Transaction, params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(t: Transaction, callback?: (err: Error, row: any) => void): Promise<number>;

  public resetAsync(t: Transaction, ): Promise<void>;

  public finalizeAsync(t: Transaction, ): Promise<void>;

  public runAsync(t: Transaction, params?: any): Promise<Statement>;

  public getAsync(t: Transaction, params?: any): Promise<any>;
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

  public runAsync(t: Transaction, sql: string): Promise<sqlite.Statement>;
  public runAsync(t: Transaction, sql: string, ...params: any[]): Promise<sqlite.Statement>;
  public runAsync(t: Transaction, sql: string, params: any): Promise<sqlite.Statement>;

  public allAsync(t: Transaction, sql: string): Promise<any[]>;
  public allAsync(t: Transaction, sql: string, ...params: any[]): Promise<any[]>;
  public allAsync(t: Transaction, sql: string, params: any): Promise<any[]>;

  public getAsync(t: Transaction, sql: string, params: any): Promise<any>;
  public getAsync(t: Transaction, sql: string, ...params: any[]): Promise<any>;
  public getAsync(t: Transaction, sql: string): Promise<any>;

  public eachAsync(t: Transaction, sql: string, params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(t: Transaction, sql: string, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(t: Transaction, ...params: any[]): Promise<number>;

  public prepareAsync(t: Transaction, sql: string, params: any): Promise<Statement>;
  public prepareAsync(t: Transaction, sql: string, ...params: any[]): Promise<Statement>;
  public prepareAsync(t: Transaction, sql: string): Promise<Statement>;

  public eachStream(t: Transaction, sql: string, ...params: any[]): Stream;

  public beginTransaction(): Promise<Transaction>;
  public commit(t: Transaction): Promise<void>;
  public rollback(t: Transaction): Promise<void>;
}

function transactionise(semaphore: Semaphore, obj: any, names: string[]) {
  for (let name of names) {
    const oldF = obj[name + "Async"].bind(obj);
    obj[name + "Async"] = async function(...params: any[]) {
      return new Promise( async (resolve, reject) => {
        if (params[0] instanceof Transaction) {
          const t = params.shift();
          if (!t.isOpen) {
            throw new Error("Transaction is closed");
          }
          resolve(oldF(...params));
        } else {
          try {
            await semaphore.wait();
            const result = await oldF(...params); 
            await semaphore.release();
            resolve(result);
          } catch (error) {
            reject(error);
          }
        }
      });
    }
  }
}

export class Transaction {
  semaphore: Semaphore;
  isOpen: boolean;
  database: Database;

  constructor(database: Database, semaphore: Semaphore) {
    this.database = database;
    this.semaphore = semaphore;
    this.isOpen = false;
  }

  async begin(): Promise<Transaction> {
    await this.semaphore.wait();
    this.isOpen = true;
    await this.database.execAsync(this, 'BEGIN TRANSACTION');
    return this;
  } 

  async commit() {
    await this.database.execAsync(this, 'COMMIT');
    await this.semaphore.release();
    this.isOpen = false;
  }

  async rollback() {
    await this.database.execAsync(this, 'ROLLBACK');
    this.isOpen = false;
    await this.semaphore.release();
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
      obj.beginTransaction = () => {
        const t = new Transaction(obj, semaphore);
        t.semaphore = semaphore;
        return t.begin();
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
