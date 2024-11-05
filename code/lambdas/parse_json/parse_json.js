console.log('Loading function');

const lambda_handler = async (event) => {
  try {
    // Parse the incoming JSON records
    if (!event.records || !Array.isArray(event.records)) {
      throw new Error("Invalid input: 'records' should be an array.");
    }

    // Loop through each record
    event.records.forEach((record, index) => {
      console.log(`Record ${index + 1}:`, JSON.stringify(record, null, 2));
    });

    // Optionally, return some response
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Records processed successfully",
        recordCount: event.records.length
      })
    };

  } catch (error) {
    console.error("Error processing records:", error.message);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Failed to process records",
        error: error.message
      })
    };
  }
};

exports.handler = lambda_handler;
