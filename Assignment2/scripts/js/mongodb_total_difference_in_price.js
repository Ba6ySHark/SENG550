db.orders_summary.aggregate([
  { $group: {
      _id: null,
      total_price_minus_amount: { $sum: { $subtract: ["$product_price", "$amount"] } }
  }},
  { $project: { _id: 0, total_price_minus_amount: 1 } }
])