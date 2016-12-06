import { Database, TransactionOptions } from '../../../';
import test from 'ava';

export async function generateArthurDatabase(): Promise<Database> {
  const db = new Database(':memory:');

  const t = await db.beginTransaction();

  await t.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await t.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Bart", "Simpson");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Smith");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Lowe");
  await t.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});

  await t.commit();

  return db;
}
