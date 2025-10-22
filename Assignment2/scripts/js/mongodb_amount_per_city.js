db.orders_summary.aggregate([
  { $group: {
      _id: "$customer_city",
      total_amount: { $sum: "$amount" }
  }},
  { $project: {
      _id: 0,
      city: "$_id",
      total_amount: 1
  }},
  { $sort: { total_amount: -1 } }
])