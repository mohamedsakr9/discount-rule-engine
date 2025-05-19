import java.time.{LocalDate, LocalDateTime, Period}
import scala.util.{Try, Using}
import java.time.format.DateTimeFormatter
import scala.io.Source
import java.io.{FileWriter, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}

// DOMAIN MODEL SECTION
// Represents a raw transaction with all necessary fields for discount processing
case class Transaction(
                        timestamp: LocalDate,
                        productName: String,
                        expiryDate: LocalDate,
                        quantity: Int,
                        unitPrice: Float,
                        channel: String,
                        paymentMethod: String
                      )

// Enriched transaction that includes discount information
// Contains the original transaction plus discount calculations
case class DiscountedTransaction(
                                  transaction: Transaction,
                                  appliedDiscounts: List[(String, Double)],  // List of all applicable discounts with their names and values
                                  finalDiscount: Double,                     // Final calculated discount after applying combination rules
                                  finalPrice: Double                         // Final price after discount is applied
                                )

// Represents a log entry for system operations
// Used for tracking application flow and debugging
case class LogEntry(
                     timestamp: LocalDateTime,
                     logLevel: String,
                     message: String
                   ) {
  // Formats log entry into standard timestamp-based format
  def format: String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    s"${timestamp.format(formatter)} $logLevel $message"
  }
}

// DISCOUNT RULES
// Contains all business logic for calculating various types of discounts
object DiscountRules {
  // Helper function to calculate days until product expiry
  val calculateDaysUntilExpiry: (LocalDate, LocalDate) => Int = (transactionDate, expiryDate) =>
    Period.between(transactionDate, expiryDate).getDays

  // Applies discount based on product expiry date
  // Products closer to expiry get higher discounts (1% per day for last 30 days)
  val expiryDateDiscount: Transaction => Option[(String, Double)] = transaction => {
    val daysRemaining = calculateDaysUntilExpiry(transaction.timestamp, transaction.expiryDate)
    if (daysRemaining < 30 && daysRemaining >= 0) {
      val discount = (30 - daysRemaining).toDouble / 100.0
      Some(("ExpiryDateDiscount", discount))
    } else None
  }

  // Applies discount based on product type
  val productTypeDiscount: Transaction => Option[(String, Double)] = transaction => {
    val productName = transaction.productName.toLowerCase
    if (productName.contains("cheese")) Some(("CheeseDiscount", 0.10))
    else if (productName.contains("wine")) Some(("WineDiscount", 0.05))
    else None
  }

  // Special promotional discount for specific date (March 23rd)
  val specialDayDiscount: Transaction => Option[(String, Double)] = transaction => {
    if (transaction.timestamp.getDayOfMonth == 23 && transaction.timestamp.getMonthValue == 3)
      Some(("SpecialDayDiscount", 0.50))
    else None
  }

  val quantityDiscount: Transaction => Option[(String, Double)] = transaction => {
    if (transaction.quantity >= 6 && transaction.quantity <= 9) Some(("QuantityDiscount", 0.05))
    else if (transaction.quantity >= 10 && transaction.quantity <= 14) Some(("QuantityDiscount", 0.07))
    else if (transaction.quantity >= 15) Some(("QuantityDiscount", 0.10))
    else None
  }
  // Discount applied for transactions made through the mobile app
  val appDiscount: Transaction => Option[(String, Double)] = transaction => {
    if (transaction.channel == "App") {
      transaction.quantity match {
        case q if q >= 1 && q <= 5 => Some(("AppDiscount", 0.05))
        case q if q >= 6 && q <= 10 => Some(("AppDiscount", 0.10))
        case q if q >= 11 && q <= 15 => Some(("AppDiscount", 0.15))
        case q if q >= 16 => Some(("AppDiscount", 0.20))
        case _ => None
      }
    }
    else None
  }

  // Discount for using Visa payment method
  val visaDiscount: Transaction => Option[(String, Double)] = transaction => {
    if (transaction.paymentMethod == "Visa")
      Some(("VisaDiscount", 0.05))
    else None
  }

  // Helper function that compares two discounts and returns the one with higher value
  // used to get the max discount between app and quantity discount as both of them rely on the quantity
  val maxDiscount: (Option[(String, Double)], Option[(String, Double)]) => Option[(String, Double)] = (a, b) => {
    (a, b) match {
      case (Some(discountA), Some(discountB)) => if (discountA._2 > discountB._2) a else b
      case (Some(_), None) => a
      case (None, Some(_)) => b
      case _ => None
    }
  }

  // Collects all applicable discounts for a transaction
  // Note: quantity and app discounts are mutually exclusive - only the higher one applies
  val getApplicableDiscounts: Transaction => List[(String, Double)] = transaction => {
    List(
      expiryDateDiscount(transaction),
      productTypeDiscount(transaction),
      specialDayDiscount(transaction),
      maxDiscount(quantityDiscount(transaction), appDiscount(transaction)),
      visaDiscount(transaction)
    ).flatten
  }

  // Business rule for final discount calculation
  // If multiple discounts apply, uses average of top two discounts
  val calculateFinalDiscount: List[(String, Double)] => Double = discounts => {
    discounts match {
      case Nil => 0.0
      case discount :: Nil => discount._2
      case _ =>
        val topTwoDiscounts = discounts.sortBy(-_._2).take(2)
        topTwoDiscounts.map(_._2).sum / topTwoDiscounts.size
    }
  }

  // Applies final discount to original price
  val calculateFinalPrice: (Float, Double) => Double = (price, discount) =>
    price * (1 - discount)

  // Main discount processing function
  // Orchestrates the full discount calculation process
  val applyDiscounts: Transaction => DiscountedTransaction = transaction => {
    val allDiscounts = getApplicableDiscounts(transaction)
    val finalDiscount = calculateFinalDiscount(allDiscounts)
    val finalPrice = calculateFinalPrice(transaction.unitPrice, finalDiscount)

    DiscountedTransaction(
      transaction = transaction,
      appliedDiscounts = allDiscounts,
      finalDiscount = finalDiscount,
      finalPrice = finalPrice
    )
  }
}

// DATA TRANSFORMATION
// Responsible for CSV parsing and data conversion operations
object DataTransformation {
  // Parses a single CSV line into a Transaction object
  // Returns Either with Transaction or error
  val parseCSVLine: (String, String) => Either[Throwable, Transaction] = (header, line) => {
    val headerFields = header.split(",").map(_.trim)
    val dataFields = line.split(",").map(_.trim)
    val fieldMap = headerFields.zip(dataFields).toMap

    Try {
      val expiryFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

      Transaction(
        timestamp = LocalDate.parse(fieldMap("timestamp").substring(0, 10)),
        productName = fieldMap("product_name"),
        expiryDate = LocalDate.parse(fieldMap("expiry_date"), expiryFormat),
        quantity = fieldMap("quantity").toInt,
        unitPrice = fieldMap("unit_price").toFloat,
        channel = fieldMap("channel"),
        paymentMethod = fieldMap("payment_method")
      )
    }.toEither
  }

  // Processes all CSV lines, skipping the header
  // Returns Either with list of Transaction results or error
  val processCSVLines: List[String] => Either[Throwable, List[Either[Throwable, Transaction]]] = lines => {
    lines match {
      case Nil => Right(Nil)
      case header :: dataLines =>
        Right(dataLines.map(line => parseCSVLine(header, line)))
    }
  }

  // High-level function to read and parse a CSV file
  // Handles file I/O and delegates parsing to other functions
  def readAndParseCSVFile(filePath: String): Either[Throwable, List[Either[Throwable, Transaction]]] = {
    Using(Source.fromFile(filePath)) { source =>
      source.getLines().toList
    }.toEither.flatMap(processCSVLines)
  }
}

// I/O OPERATIONS
// Handles input/output operations like logging and database interaction
object IOOperations {
  // Creates a log entry with current timestamp
  val createLogEntry: (String, String) => LogEntry = (level, message) =>
    LogEntry(LocalDateTime.now, level, message)

  // Writes a log entry to the specified file
  // Returns Either with Unit on success or Throwable on error
  def writeLogEntry(entry: LogEntry, filePath: String): Either[Throwable, Unit] = {
    Try {
      val writer = new PrintWriter(new FileWriter(filePath, true))
      try {
        writer.println(entry.format)
      } finally {
        writer.close()
      }
    }.toEither
  }

  // Creates a database connection
  // Returns Either with Connection on success or Throwable on error
  def createConnection(jdbcUrl: String): Either[Throwable, Connection] = {
    Try {
      Class.forName("org.postgresql.Driver")
      DriverManager.getConnection(jdbcUrl, "user", "123")
    }.toEither
  }

  // Saves a discounted transaction to the database
  // Returns Either with Unit on success or Throwable on error
  def saveTransaction(connection: Connection, transaction: DiscountedTransaction): Either[Throwable, Unit] = {
    Try {
      val preparedStatement = connection.prepareStatement(
        """
        INSERT INTO discounted_transactions
        (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, final_price, applied_discounts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
      )
      // Only stores the top two discount rule names for simplicity
      val topTwoRules = transaction.appliedDiscounts
        .sortBy(-_._2)
        .take(2)
        .map(_._1)
        .mkString(",")

      preparedStatement.setString(1, transaction.transaction.timestamp.toString)
      preparedStatement.setString(2, transaction.transaction.productName)
      preparedStatement.setString(3, transaction.transaction.expiryDate.toString)
      preparedStatement.setInt(4, transaction.transaction.quantity)
      preparedStatement.setFloat(5, transaction.transaction.unitPrice)
      preparedStatement.setString(6, transaction.transaction.channel)
      preparedStatement.setString(7, transaction.transaction.paymentMethod)
      preparedStatement.setDouble(8, transaction.finalDiscount)
      preparedStatement.setDouble(9, transaction.finalPrice)
      preparedStatement.setString(10, topTwoRules)
      preparedStatement.executeUpdate()
      ()
    }.toEither
  }
}

// MAIN APPLICATION
// Entry point and main workflow orchestration
object DiscountRuleEngine extends App {
  val csvFilePath = "D:/45/ScalaProject/TRX1000.csv"
  val logFilePath = "rules_engine.log"
  val dbUrl = "jdbc:postgresql://localhost:5444/scala"

  processTransactions()

  // Main processing function that orchestrates the entire workflow
  def processTransactions(): Unit = {
    // Log application start
    val startLog = IOOperations.createLogEntry("INFO", "Starting transaction processing")
    IOOperations.writeLogEntry(startLog, logFilePath) match {
      case Left(error) =>
        // Early exit if logging fails - critical error
        println(s"Failed to initialize logging: ${error.getMessage}")
        error.printStackTrace()
        return
      case Right(_) => // Continue processing
    }

    // Log reading CSV file
    IOOperations.writeLogEntry(
      IOOperations.createLogEntry("INFO", s"Reading CSV from: $csvFilePath"),
      logFilePath
    )

    // Main workflow begins - read and parse CSV file
    DataTransformation.readAndParseCSVFile(csvFilePath) match {
      case Right(transactionResults) =>
        // Calculate and log success/failure counts
        val successCount = transactionResults.count(_.isRight)
        val failureCount = transactionResults.count(_.isLeft)

        // Log parsing results
        IOOperations.writeLogEntry(
          IOOperations.createLogEntry("INFO", s"Parsed ${transactionResults.size} transactions. Successful: $successCount, Failed: $failureCount"),
          logFilePath
        )

        // Log database connection attempt
        IOOperations.writeLogEntry(
          IOOperations.createLogEntry("INFO", "Connecting to database"),
          logFilePath
        )

        // Create database connection
        IOOperations.createConnection(dbUrl) match {
          case Right(connection) =>
            // Log successful connection
            IOOperations.writeLogEntry(
              IOOperations.createLogEntry("INFO", "Database connection successful"),
              logFilePath
            )

            try {
              // Log start of transaction processing
              IOOperations.writeLogEntry(
                IOOperations.createLogEntry("INFO", "Beginning transaction processing"),
                logFilePath
              )

              // Process each transaction individually
              transactionResults.foreach {
                case Right(transaction) =>
                  // Apply discount rules to this transaction
                  val discountedTransaction = DiscountRules.applyDiscounts(transaction)

                  // Log discount application
                  IOOperations.writeLogEntry(
                    IOOperations.createLogEntry("INFO",
                      s"Applied discount of ${(discountedTransaction.finalDiscount * 100).toInt}% to ${transaction.productName}"),
                    logFilePath
                  )

                  // Save to database and log result
                  IOOperations.saveTransaction(connection, discountedTransaction) match {
                    case Left(error) =>
                      // Log transaction save error
                      IOOperations.writeLogEntry(
                        IOOperations.createLogEntry("ERROR",
                          s"Failed to save transaction for ${transaction.productName}: ${error.getMessage}"),
                        logFilePath
                      )
                    case Right(_) =>
                      // Log successful transaction save
                      IOOperations.writeLogEntry(
                        IOOperations.createLogEntry("INFO",
                          s"Saved transaction for ${transaction.productName}"),
                        logFilePath
                      )
                  }

                case Left(error) =>
                  // Log invalid transaction
                  IOOperations.writeLogEntry(
                    IOOperations.createLogEntry("ERROR",
                      s"Skipping invalid transaction: ${error.getMessage}"),
                    logFilePath
                  )
              }

              // Log completion of database operations
              IOOperations.writeLogEntry(
                IOOperations.createLogEntry("INFO", "Database operations completed"),
                logFilePath
              )

            } finally {
              // Log database connection closing - always runs, even if errors occur
              IOOperations.writeLogEntry(
                IOOperations.createLogEntry("INFO", "Closing database connection"),
                logFilePath
              )
              connection.close()
            }

          case Left(error) =>
            // Log database connection error
            IOOperations.writeLogEntry(
              IOOperations.createLogEntry("ERROR",
                s"Failed to connect to database: ${error.getMessage}"),
              logFilePath
            )
        }

        // Log completion
        IOOperations.writeLogEntry(
          IOOperations.createLogEntry("INFO", "Processing complete"),
          logFilePath
        )

      case Left(error) =>
        // Log CSV file read error
        IOOperations.writeLogEntry(
          IOOperations.createLogEntry("ERROR", s"Failed to read CSV file: ${error.getMessage}"),
          logFilePath
        )
    }
  }
}