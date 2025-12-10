const fs = require('fs');
const path = require('path');
const { faker } = require('@faker-js/faker');

const DATA_DIR = path.join(__dirname, 'data');

const NUM_USERS = 50;
const NUM_PRODUCTS = 25;
const NUM_ORDERS = 120;

const randomChoice = (arr) => arr[Math.floor(Math.random() * arr.length)];
const formatCurrency = (value) => Number(value.toFixed(2));

const ensureDir = (dir) => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
};

const writeCsv = (filename, headers, rows) => {
  const content = [headers.join(','), ...rows].join('\n');
  fs.writeFileSync(path.join(DATA_DIR, filename), content);
};

const generateUsers = () => {
  const users = [];
  for (let i = 1; i <= NUM_USERS; i += 1) {
    users.push({
      id: i,
      name: faker.person.fullName(),
      email: faker.internet.email(),
      created_at: faker.date.past({ years: 2 }).toISOString(),
    });
  }
  return users;
};

const generateProducts = () => {
  const categories = ['Electronics', 'Home', 'Toys', 'Books', 'Fashion', 'Sports'];
  const products = [];
  for (let i = 1; i <= NUM_PRODUCTS; i += 1) {
    const basePrice = faker.number.float({ min: 5, max: 500, precision: 0.01 });
    products.push({
      id: i,
      name: faker.commerce.productName(),
      category: randomChoice(categories),
      price: formatCurrency(basePrice),
    });
  }
  return products;
};

const generateOrders = (users) => {
  const orders = [];
  for (let i = 1; i <= NUM_ORDERS; i += 1) {
    const user = randomChoice(users);
    const orderDate = faker.date.between({ from: user.created_at, to: new Date() });
    orders.push({
      id: i,
      user_id: user.id,
      order_date: orderDate.toISOString(),
      total_amount: 0, // placeholder, will be filled after items
    });
  }
  return orders;
};

const generateOrderItems = (orders, products) => {
  const orderItems = [];
  orders.forEach((order) => {
    const itemCount = faker.number.int({ min: 1, max: 5 });
    let orderTotal = 0;

    for (let j = 1; j <= itemCount; j += 1) {
      const product = randomChoice(products);
      const quantity = faker.number.int({ min: 1, max: 5 });
      const itemPrice = product.price; // lock to product price for consistency
      const lineTotal = formatCurrency(itemPrice * quantity);
      orderTotal = formatCurrency(orderTotal + lineTotal);

      orderItems.push({
        id: orderItems.length + 1,
        order_id: order.id,
        product_id: product.id,
        quantity,
        item_price: itemPrice,
      });
    }

    order.total_amount = orderTotal;
  });

  return orderItems;
};

const generatePayments = (orders) => {
  const paymentMethods = ['card', 'paypal', 'bank_transfer', 'apple_pay', 'google_pay'];
  const paymentStatuses = ['paid', 'pending', 'failed', 'refunded'];
  const payments = [];

  orders.forEach((order) => {
    const paymentDate = faker.date.between({
      from: order.order_date,
      to: new Date(),
    });

    payments.push({
      id: payments.length + 1,
      order_id: order.id,
      payment_method: randomChoice(paymentMethods),
      payment_status: randomChoice(paymentStatuses),
      payment_date: paymentDate.toISOString(),
    });
  });

  return payments;
};

const toCsvRow = (fields) =>
  fields
    .map((field) => {
      if (typeof field === 'string' && field.includes(',')) {
        return `"${field.replace(/"/g, '""')}"`;
      }
      return field;
    })
    .join(',');

const main = () => {
  ensureDir(DATA_DIR);

  const users = generateUsers();
  const products = generateProducts();
  const orders = generateOrders(users);
  const orderItems = generateOrderItems(orders, products);
  const payments = generatePayments(orders);

  writeCsv(
    'users.csv',
    ['id', 'name', 'email', 'created_at'],
    users.map((u) => toCsvRow([u.id, u.name, u.email, u.created_at]))
  );

  writeCsv(
    'products.csv',
    ['id', 'name', 'category', 'price'],
    products.map((p) => toCsvRow([p.id, p.name, p.category, p.price]))
  );

  writeCsv(
    'orders.csv',
    ['id', 'user_id', 'order_date', 'total_amount'],
    orders.map((o) => toCsvRow([o.id, o.user_id, o.order_date, o.total_amount]))
  );

  writeCsv(
    'order_items.csv',
    ['id', 'order_id', 'product_id', 'quantity', 'item_price'],
    orderItems.map((oi) =>
      toCsvRow([oi.id, oi.order_id, oi.product_id, oi.quantity, oi.item_price])
    )
  );

  writeCsv(
    'payments.csv',
    ['id', 'order_id', 'payment_method', 'payment_status', 'payment_date'],
    payments.map((p) =>
      toCsvRow([p.id, p.order_id, p.payment_method, p.payment_status, p.payment_date])
    )
  );

  console.log('Synthetic CSV data generated in ./data');
};

main();

