db.orders_summary.aggregate([
  { $match: { customer_id: "C1" } },
  { $project: {
      _id: 0,
      order_id: 1,
      order_date: 1,
      customer_city: 1,
      product_name: 1,
      product_price: 1,
      amount: 1
  }},
  { $sort: { order_date: 1, order_id: 1 } }
])