## Squeamish

A minimal wrapper around the node [sqlite3 module](https://github.com/mapbox/node-sqlite3/)
to provide basic TypeScript and Promise (async/await) support. It also includes
support for transactions and RxJS Observables. Of course, it works with plain
old JavaScript too!

## Transactions

Obviously with any SQLite connection you could create a transaction using the
SQLite `BEGIN` syntax.  However, if your code is running asynchronously,
there's nothing there to stop other events causing statements to be executed
within your transaction.  Squeamish generates a logical database handle within
the transaction for your queries to execute on, and locks the original handle
until the transaction is closed. It also supports nested transactions using the
`SAVEPOINT` syntax.

## Basic Usage

Promisification follows the convention of `Bluebird.promisifyAll`, so the API
follows the [standard sqlite3 module](https://github.com/mapbox/node-sqlite3/wiki/API), with `Async` appended to method names and
the final callback removed. 
Note that prepared statement usage is different, see the example below:

## Example

```typescript
import { Database, Statement } from 'squeamish';
    
async function testDB() {
  const db = new Database(':memory:');

  await db.execAsync('CREATE TABLE people (firstname TEXT, lastname TEXT);');

  await db.runAsync('INSERT INTO people VALUES ("Jeff", "Smith");');
  await db.runAsync('INSERT INTO people VALUES (?, ?);', ["Bart", "Simpson"]);
  await db.runAsync('INSERT INTO people VALUES (?, ?);', "Arthur", "Dent");

  const statement = await db.prepareAsync('SELECT * from people;');

  // Unlike the sqlite3 module, statements are passed to the db handle
  // (or transaction)
  let numRows = await db.eachAsync(statement, (err, row) => {
    console.log("Person is", row.firstname, row.lastname);
  });

  console.log("There were", numRows, "people");

  // Transactions:
  const t = await db.beginTransaction();
  // Use the tranaction like a DB connection
  await t.runAsync('INSERT INTO people VALUES ("Fred", "Flintstone");');

  // Note that await db.runAsync('...'); will block while the transaction is open
  // You are able to open additional Database() objects however and use those.

  // Nesting transactions

  const t2 = await t.beginTransaction();
  numRows = await t2.eachAsync(statement, (err, row) => {
    console.log("Person is", row.firstname, row.lastname);
  });

  console.log("There are now", numRows, "people");

  await t2.commit(); //OR rollback();
  await t.commit();

  // RxJS 5 Observable support

  await db.select('SELECT firstname from people')
    .map(x => x.firstname)
    .map(name => "Hi, my name is " + name)
    .do(console.log)
    .toPromise();

}

testDB().then(() => {
  console.log("Finished");
}).catch( err => {
  console.error("Error:", err);
});
```
