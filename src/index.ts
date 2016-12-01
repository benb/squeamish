import * as sqlite from 'sqlite3';
import * as Bluebird from 'bluebird';
import * as uuid from 'uuid';
import { Semaphore } from 'prex';
import { Observable, Observer } from '@reactivex/rxjs';

export declare class Statement extends sqlite.Statement {
  public bindAsync(params: any): Promise<void>;

  public eachAsync(params: any, callback: (err: Error, row: any) => void): Promise<number>;
  public eachAsync(callback?: (err: Error, row: any) => void): Promise<number>;

  public resetAsync(): Promise<void>;

  public finalizeAsync(): Promise<void>;

  public runAsync(params?: any): Promise<Statement>;

  public getAsync(params?: any): Promise<any>;

}

export interface Handle {
    runAsync: (sql: string, ...params: any[]) => Promise<sqlite.Statement>;
    allAsync: (sql: string, ...params: any[]) => Promise<any[]>;
    getAsync: (sql: string, ...params: any[]) => Promise<any>;
    eachAsync: (sql: string, ...params: any[]) => Promise<number>;
    execAsync: (sql: string, ...params: any[]) => Promise<any>;
    select(sql: string, ...params: any[]): Observable<any>;
    beginTransaction(options?: TransactionOptions): Promise<Transaction>;
}

function withinLock<T>(f:() => Promise<T>, semaphore: Semaphore): Promise<T> {
  return semaphore.wait()
  .then(() => f())
  .catch(async err => {
    await semaphore.release();
    throw err;
  })
  .then(async result => {
    await semaphore.release();
    return result;
  });
}

function observableWithinLock<T>(obs: Observable<T>, semaphore: Semaphore) {
  obs.do({complete: () => {this.semaphore.release()}});
  return Observable.from(this.semaphore.wait()).concat(obs).skip(1);
}

function lockF<T>( f:() => ((sql: string, ...params: any[]) => Promise<T>), semaphore: {semaphore: Semaphore}) {
  return (sql: string, ...params: any[]) => {
    return withinLock(() => f()(sql, ...params), semaphore.semaphore);
  };
}

export class Database implements Handle {
  sqlite: sqlite.Database;
  semaphore: Semaphore;

  constructor(path: string, mode: number = sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE) {
    this.sqlite = new sqlite.Database(path, mode);
    this.semaphore = createSemaphore();
  }
 
  private promisifyF<T>( f:() => ((sql: string, ...params: any[]) => void)) {
    return (sql: string, ...params: any[]) => {
      return new Promise<T>((resolve, reject) => {
        f()(sql, ...[...params, (err: Error, result: T) => {
          if (err) {reject(err)} else {resolve(result)} 
        }]);
      });
    };
  }

  _select(sql: string, ...params: any[]) {
    return Observable.create((observer: Observer<any>) =>  {
      const f = (e: Error, r: any) => {
        if (e){observer.error(e)}
        else {observer.next(r)}
      };
      this._eachAsync(sql, f, params)
          .then(n => observer.complete());
    });
  }

  select(sql: string, ...params: any[]) {
    return observableWithinLock(this._select(sql, ...params), this.semaphore);
  }

  public closeAsync():Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.sqlite.close((err: Error) => {
        if (err) {reject(err);} else {resolve();}
      });
    });
 }

 _runAsync(sql: string, ...params: any[]): Promise<sqlite.Statement> {
   return new Promise<sqlite.Statement>((resolve, reject) => {
     this.sqlite.run(sql, ...[...params, (err: Error, s: sqlite.Statement) => {
       if (err) {reject(err)} else {resolve(s)} 
     }]);
   });
 }
 _allAsync(sql: string, ...params: any[]) {
   return this._select(sql, ...params).toArray().toPromise();
   //return new Promise<any[]>((resolve, reject) => {
     //this.sqlite.all.bind(this.sqlite,sql, ...params)((err: Error, rows: any[]) => {
       //if (err) {reject(err)} else {resolve(rows)} 
     //});
   //});
 }

 _getAsync(sql: string, ...params: any[]) {
    return new Promise<any>((resolve, reject) => {
     this.sqlite.get.bind(this.sqlite,sql, ...params)((err: Error, row: any) => {
       if (err) {reject(err)} else {resolve(row)} 
     });
   });
 }

  public runAsync(sql: string, ...params: any[]) {
    return withinLock(this._runAsync.bind(this, sql, ...params), this.semaphore);
  }

  public allAsync(sql: string, ...params: any[]): Promise<any[]> {
    return withinLock(this._allAsync.bind(this, sql, ...params), this.semaphore);
  }

  public getAsync(sql: string, ...params: any[]): Promise<any> {
    return withinLock(this._getAsync.bind(this, sql, ...params), this.semaphore);
  }

  public prepareAsync:(sql: string, ...params: any[]) => Promise<Statement> = this.promisifyF(() => this.sqlite.prepare.bind(this.sqlite));

 _eachAsync(sql: string, callback: (err: Error, row: any) => void, ...params:any[]): Promise<number> {
   return new Promise<number>((resolve, reject) => {
     this.sqlite.each(sql, params, callback, (err: Error, result: number) => {
       if (err) {reject(err)} else {resolve(result)} 
     });
   });
 }

 public eachAsync = lockF(() => this._eachAsync.bind(this), this);

 public _execAsync(sql: string) {
   return new Promise<void>((resolve, reject) => {
     this.sqlite.exec(sql, (err: Error) => {
       if (err) {reject(err)} else {resolve()} 
     });
   });
 }

 public execAsync = lockF(() => this._execAsync.bind(this), this);

 public beginTransaction(options?: TransactionOptions): Promise<Transaction> {
   return Transaction.begin(this as any, this.semaphore, options);
 }
}

export type TransactionOptions = {type: "IMMEDIATE" | "DEFERRED" | "EXCLUSIVE" };

const debug = false;

function createSemaphore() {
  const s = new Semaphore(1);
  if (debug) {
    const wait = s.wait;
    s.wait = async () => {
      console.trace("ACQUIRING SEMAPHORE LOCK");
      let cancelT = false;
      setTimeout(() => {
        if (!cancelT) {console.log("SEMAPHORE BLOCKED?");}
      }, 2000);
      await wait.bind(s)();
      cancelT = true;
    }
  }
  return s;
}

export class Transaction implements Handle {
  externalSemaphore: Semaphore;
  internalSemaphore: Semaphore;
  options?: TransactionOptions;
  isOpen: boolean;
  database: Database;
  savepoint?: string;

  get semaphore() {
    return this.internalSemaphore;
  }

  constructor(database: Database, semaphore: Semaphore, options?: TransactionOptions) {
    this.database = database;
    this.externalSemaphore = semaphore;
    this.internalSemaphore = createSemaphore();
    this.isOpen = false;
    this.options = options;
    this.execAsync = lockF(() => this.database._execAsync.bind(this.database), this);
    this.runAsync = lockF(() => this.database._runAsync.bind(this.database), this);
    this.allAsync = lockF(() => this.database._allAsync.bind(this.database), this);
    this.getAsync = lockF(() => this.database._getAsync.bind(this.database), this);
    this.eachAsync = lockF(() => this.database._eachAsync.bind(this.database), this);
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
    this.savepoint = uuid.v4();
    if (this.options) {
      const type = this.options ? this.options.type : "DEFERRED";
      await this.database._execAsync(`BEGIN ${type} TRANSACTION`);
    } else {
      await this.database._execAsync(`SAVEPOINT "${this.savepoint}"`);
    }
    this.isOpen = true;
    return this;
  }

  async beginTransaction(options?: TransactionOptions): Promise<Transaction> {
    const internalTransaction = new Transaction(this.database, this.internalSemaphore);
    const t = await internalTransaction.begin();
    return t;
  } 

  async commit() {
    if (this.options) {
      await this.database._execAsync(`COMMIT`);
    } else {
      await this.database._execAsync(`RELEASE '${this.savepoint}'`);
    }
    await this.externalSemaphore.release();
  }

  async rollback() {
    if (this.options) {
      await this.database._execAsync(`ROLLBACK`);
    } else {
      await this.database._execAsync(`ROLLBACK TO '${this.savepoint}'`);
    }
    await this.externalSemaphore.release();
  }

  runAsync: (sql: string, ...params: any[]) => Promise<sqlite.Statement>;
  allAsync: (sql: string, ...params: any[]) => Promise<any[]>;
  getAsync: (sql: string, ...params: any[]) => Promise<any[]>;
  eachAsync: (sql: string, ...params: any[]) => Promise<number>;
  execAsync: (sql: string, ...params: any[]) => Promise<any>;

  select(sql: string, ...params: any[]) {
    return observableWithinLock(this.database._select(sql, ...params), this.semaphore);
  }
}

