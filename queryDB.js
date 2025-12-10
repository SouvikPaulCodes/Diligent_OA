const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const dbPath = path.join(__dirname, 'ecommerce.db');
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error('Failed to open database:', err.message);
    process.exit(1);
  }
});

const query = `
  SELECT
    u.id AS user_id,
    u.name AS user_name,
    o.id AS order_id,
    o.order_date,
    p.name AS product_name,
    oi.quantity,
    oi.item_price,
    o.total_amount
  FROM orders o
  JOIN users u ON o.user_id = u.id
  JOIN order_items oi ON oi.order_id = o.id
  JOIN products p ON p.id = oi.product_id
  ORDER BY o.order_date DESC
`;

db.all(query, [], (err, rows) => {
  if (err) {
    console.error('Query failed:', err.message);
    db.close();
    process.exit(1);
  }

  if (!rows.length) {
    console.log('No results found.');
    db.close();
    return;
  }

  console.table(rows);
  db.close();
});
