db.orders_summary.aggregate([
  { $group: {
      _id: "$customer_id",
      cities: { $addToSet: "$customer_city" }
  }},
  { $project: {
      _id: 0,
      customer_id: "$_id",
      city_count: { $size: "$cities" }
  }},
  { $sort: { customer_id: 1 } }
])