db.air_alliances.aggregate(
  [
    {
      $lookup: {
        from: "air_routes",
        localField: "airlines",
        foreignField: "airline.name",
        as: "air_routes_air_alliances",
      },
    },
    {
      $unwind: "$air_routes_air_alliances",
    },
    {
      $match: {
        "air_routes_air_alliances.airplane": {
          $in: ["747", "380"],
        },
      },
    },
    {
      $group: {
        _id: "$name",
        totalRotas: {
          $sum: 1,
        },
      },
    },
    {
      $sort: {
        totalRotas: -1,
      },
    },
    {
      $limit: 1,
    },
  ],
);
