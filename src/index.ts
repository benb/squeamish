import * as sqlite from 'sqlite3';
import * as Bluebird from 'bluebird';
import * as uuid from 'uuid';
import { Semaphore } from 'prex';
import { Observable, Observer } from 'rxjs';
const debug = false;

export class Statement {
  stmt: sqlite.Statement;

  constructor(stmt: sqlite.Statement) {
    this.stmt = stmt;
  }

  public bindAsync(...params: any[]): Promise<Statement> {
    return new Promise((resolve, reject) => {
      this.stmt.bind(...params, (err: Error) => {
        if (err) {reject(err)} else {resolve(this)} 
      });
    });
  }

  public resetAsync(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.stmt.reset(err => {
        if (err) {reject(err)} else {resolve()}
      });
    });
  }

  public finalizeAsync(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.stmt.finalize(err => {
        if (err) {reject(err)} else {resolve()}
      });
    });
  }

}

export interface Handle {
    runAsync(sql: string, ...params: any[]): Promise<Statement>;
    runAsync(statement: Statement, ...params: any[]): Promise<Statement>;
    allAsync(sql: string, ...params: any[]): Promise<any[]>;
    allAsync(statement: Statement, ...params: any[]): Promise<any[]>;
    getAsync(sql: string, ...params: any[]): Promise<any>;
    getAsync(statement: Statement, ...params: any[]): Promise<any>;
    eachAsync(sql: string, ...params: any[]): Promise<number>;
    eachAsync(statement: Statement, ...params: any[]): Promise<number>;
    execAsync(sql: string, ...params: any[]): Promise<any>;
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
  obs = obs.do({
    error: () => {
      semaphore.release()
    },
    complete: () => {
      semaphore.release()}
  });
  return Observable
    .create((waiter:Observer<T>) => {
      semaphore.wait().then(() => {waiter.complete()});
    })
    .ignoreElements()
    .concat(obs);
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
 
  _select(sql: string | Statement, ...params: any[]) {
    return Observable.create((observer: Observer<any>) =>  {
      const f = (e: Error, r: any) => {
        if (e){observer.error(e)}
        else {observer.next(r)}
      };
      this._eachAsync(sql, f, ...params)
          .then(n => observer.complete())
          .catch(err => observer.error(err));
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

 _runAsync(sql: string | Statement, ...params: any[]): Promise<Statement> {
   return new Promise<Statement>((resolve, reject) => {
     const cb = (err: Error, s: sqlite.Statement) => {
       let statement = sql instanceof Statement ? sql : new Statement(s)
       if (err) {reject(err)} else {resolve(statement)} 
     }
     if (sql instanceof Statement) {
       sql.stmt.run(...params, cb);
     } else {
       this.sqlite.run(sql, ...params, cb);
     }
   });
 }

 _allAsync(sql: string | Statement, ...params: any[]) {
   return this._select(sql, ...params).toArray().toPromise();
 }

 _getAsync(sql: string | Statement, ...params: any[]) {
    return new Promise<any>((resolve, reject) => {
     const cb = (err: Error, row: any) => {
       if (err) {reject(err)} else {resolve(row)} 
     };
     if (sql instanceof Statement) {
       sql.stmt.get(...params, cb);
     } else {
       this.sqlite.get(sql, ...params, cb);
     }
   });
 }

 _eachAsync(sql: string | Statement, callback: (err: Error, row: any) => void, ...params:any[]): Promise<number> {
   return new Promise<number>((resolve, reject) => {
     const finalCB = (err: Error, result: number) => {
       if (err) {reject(err)} else {resolve(result)} 
     };

     if (sql instanceof Statement) {
       sql.stmt.each(...params, callback, finalCB);
     } else {
       this.sqlite.each(sql, ...params, callback, finalCB);
     }
   });
 }


 _execAsync(sql: string) {
   return new Promise<void>((resolve, reject) => {
     this.sqlite.exec(sql, (err: Error) => {
       if (err) {reject(err)} else {resolve()} 
     });
   });
 }

  public runAsync(sql: string | Statement, ...params: any[]) {
    return withinLock(this._runAsync.bind(this, sql, ...params), this.semaphore);
  }

  public allAsync(sql: string | Statement, ...params: any[]): Promise<any[]> {
    return withinLock(this._allAsync.bind(this, sql, ...params), this.semaphore);
  }

  public getAsync(sql: string | Statement, ...params: any[]): Promise<any> {
    return withinLock(this._getAsync.bind(this, sql, ...params), this.semaphore);
  }

  public eachAsync(sql: string | Statement, ...params: any[]): Promise<number> {
    return withinLock(this._eachAsync.bind(this, sql, ...params), this.semaphore);
  }

  public execAsync(sql: string, ...params: any[]): Promise<any> {
    return withinLock(this._execAsync.bind(this, sql, ...params), this.semaphore);
  }

  public prepareAsync(sql: string, ...params: any[]): Promise<Statement> {
    return new Promise((resolve, reject) => {
      let stmt = this.sqlite.prepare.bind(this.sqlite, sql, ...params)((err: Error) => {
        if (err) {reject(err)} else {resolve(new Statement(stmt))}
      });
    });
  }

 public beginTransaction(options?: TransactionOptions): Promise<Transaction> {
   return Transaction.begin(this as any, this.semaphore, options);
 }
}

export type TransactionOptions = {type: "IMMEDIATE" | "DEFERRED" | "EXCLUSIVE" };


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

  runAsync: (sql: string | Statement, ...params: any[]) => Promise<Statement>;
  allAsync: (sql: string | Statement, ...params: any[]) => Promise<any[]>;
  getAsync: (sql: string | Statement, ...params: any[]) => Promise<any>;
  eachAsync: (sql: string | Statement, ...params: any[]) => Promise<number>;
  execAsync: (sql: string, ...params: any[]) => Promise<any>;

  select(sql: string, ...params: any[]) {
    return observableWithinLock(this.database._select(sql, ...params), this.semaphore);
  }
}

