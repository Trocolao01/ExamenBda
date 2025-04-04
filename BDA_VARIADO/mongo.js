db.orders.aggregate([
    {
      $unwind: "$items"
    },
    {
      $lookup: {
        from: "products", // Nombre de la colección de productos
        localField: "items.product", // Campo en orders (nombre del producto)
        foreignField: "name", // Campo en products (nombre del producto)
        as: "productDetails"
      }
    },
    {
      $unwind: "$productDetails" // Para manejar la relación uno a uno
    },
    {
      $project: {
        "product": "$items.product",
        "category": "$productDetails.category",
        "quantity": "$items.quantity",
        "revenue": { $multiply: ["$items.quantity", "$items.price"] }
      }
    },
    {
      $group: {
        "_id": "$product",
        "total_quantity_sold": { $sum: "$quantity" },
        "total_revenue": { $sum: "$revenue" },
        "category": { $first: "$category" } // Mantener la categoría del producto
      }
    },
    {
      $sort: { "total_revenue": -1 }
    }
  ]);
  