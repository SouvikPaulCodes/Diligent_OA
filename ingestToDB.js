const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const dbPath = path.join(__dirname, 'ecommerce.db');
const dataDir = path.join(__dirname, 'data');
const db = new sqlite3.Database(dbPath);

const run = (sql, params = []) =>
  new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) return reject(err);
      resolve(this);
    });
  });

const prepare = (sql) =>
  new Promise((resolve, reject) => {
    const stmt = db.prepare(sql, (err) => {
      if (err) return reject(err);
      resolve(stmt);
    });
  });

const finalize = (stmt) =>
  new Promise((resolve, reject) => {
    stmt.finalize((err) => {
      if (err) return reject(err);
      resolve();
    });
  });

function loadCSV(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];

  const [headerLine, ...lines] = raw.split(/\r?\n/);
  const headers = headerLine.split(',');

  return lines
    .filter(Boolean)
    .map((line) => line.split(','))
    .map((values) => {
      const record = {};
      headers.forEach((header, idx) => {
        record[header] = values[idx];
      });
      return record;
    });
}

async function insertRows(table, columns, rows) {
  if (!rows.length) {
    console.log(`Inserted 0 rows into ${table}`);
    return;
  }

  const placeholders = columns.map(() => '?').join(',');
  const stmt = await prepare(
    `INSERT INTO ${table} (${columns.join(',')}) VALUES (${placeholders})`
  );

  for (const row of rows) {
    const values = columns.map((col) => row[col]);
    // Sequential inserts to respect FK constraints deterministically.
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve, reject) => {
      stmt.run(values, (err) => (err ? reject(err) : resolve()));
    });
  }

  await finalize(stmt);
  console.log(`Inserted ${rows.length} rows into ${table}`);
}

async function main() {
  try {
    await run('PRAGMA foreign_keys = ON');

    await run(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT,
      email TEXT,
      created_at TEXT
    )`);

    await run(`CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY,
      name TEXT,
      category TEXT,
      price REAL
    )`);

    await run(`CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER,
      order_date TEXT,
      total_amount REAL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    )`);

    await run(`CREATE TABLE IF NOT EXISTS order_items (
      id INTEGER PRIMARY KEY,
      order_id INTEGER,
      product_id INTEGER,
      quantity INTEGER,
      item_price REAL,
      FOREIGN KEY(order_id) REFERENCES orders(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    )`);

    await run(`CREATE TABLE IF NOT EXISTS payments (
      id INTEGER PRIMARY KEY,
      order_id INTEGER,
      payment_method TEXT,
      payment_status TEXT,
      payment_date TEXT,
      FOREIGN KEY(order_id) REFERENCES orders(id)
    )`);

    const users = loadCSV(path.join(dataDir, 'users.csv')).map((row) => ({
      id: Number(row.id),
      name: row.name,
      email: row.email,
      created_at: row.created_at,
    }));

    const products = loadCSV(path.join(dataDir, 'products.csv')).map((row) => ({
      id: Number(row.id),
      name: row.name,
      category: row.category,
      price: Number(row.price),
    }));

    const orders = loadCSV(path.join(dataDir, 'orders.csv')).map((row) => ({
      id: Number(row.id),
      user_id: Number(row.user_id),
      order_date: row.order_date,
      total_amount: Number(row.total_amount),
    }));

    const orderItems = loadCSV(path.join(dataDir, 'order_items.csv')).map(
      (row) => ({
        id: Number(row.id),
        order_id: Number(row.order_id),
        product_id: Number(row.product_id),
        quantity: Number(row.quantity),
        item_price: Number(row.item_price),
      })
    );

    const payments = loadCSV(path.join(dataDir, 'payments.csv')).map((row) => ({
      id: Number(row.id),
      order_id: Number(row.order_id),
      payment_method: row.payment_method,
      payment_status: row.payment_status,
      payment_date: row.payment_date,
    }));

    await insertRows('users', ['id', 'name', 'email', 'created_at'], users);
    await insertRows(
      'products',
      ['id', 'name', 'category', 'price'],
      products
    );
    await insertRows(
      'orders',
      ['id', 'user_id', 'order_date', 'total_amount'],
      orders
    );
    await insertRows(
      'order_items',
      ['id', 'order_id', 'product_id', 'quantity', 'item_price'],
      orderItems
    );
    await insertRows(
      'payments',
      ['id', 'order_id', 'payment_method', 'payment_status', 'payment_date'],
      payments
    );

    console.log('Ingestion completed.');
  } catch (err) {
    console.error('Error during ingestion:', err);
  } finally {
    db.close();
  }
}

main();
