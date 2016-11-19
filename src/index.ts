import * as sqlite from 'sqlite3';
import * as Bluebird from 'bluebird';

export declare class Statement extends sqlite.Statement {
  public eachAsync(params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(callback?: (err: Error, row: any) => void): Promise<number>;

  public bindAsync(params: any): Promise<void>;

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
}

export namespace SQLite{
  export async function open(path: string, mode: number = sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE): Promise<Database> {
    return new Promise<Database>( (resolve, reject) => {
      const db = new sqlite.Database(path, mode, (error) => {
        if (error) { reject(error); }
        else { resolve(promisify(db)); }
      });
    });
  }

  export function promisify(database: sqlite.Database): Database {
    const obj = Bluebird.promisifyAll(database) as any;

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

      return promise.then( () => { return Bluebird.promisifyAll(stmt) as Statement });
    }
    return obj;
  }
}
