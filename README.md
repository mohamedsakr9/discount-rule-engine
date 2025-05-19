Discount Rule Engine Documentation
Overview
The Discount Rule Engine is a Scala application designed to process retail transactions, apply various discount rules according to business logic, and store the results in a PostgreSQL database. It demonstrates functional programming principles in Scala while implementing a practical business use case for retail discounting.

Table of Contents
System Architecture
Domain Model
Discount Rules
Data Flow
Configuration
Installation and Setup
Usage Instructions
Error Handling
Extending the System
Troubleshooting
API Reference
System Architecture
The application follows a functional architecture with clear separation of concerns:

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  CSV Input File │────▶│ Data Processing │────▶│ PostgreSQL DB   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               │
                               ▼
                        ┌─────────────────┐
                        │  Logging File   │
                        └─────────────────┘
The system consists of four main modules:

Domain Model: Core data structures representing transactions and discount calculations
Discount Rules: Business logic for determining applicable discounts
Data Transformation: CSV parsing and data conversion
I/O Operations: File handling, logging, and database interactions
Domain Model
The domain model consists of three primary case classes:

Transaction
Represents a raw retail transaction with all necessary fields for discount processing:

timestamp: Date when the transaction occurred
productName: Name of the product purchased
expiryDate: Expiration date of the product
quantity: Number of items purchased
unitPrice: Cost per unit
channel: Sales channel (e.g., "App" for mobile application)
paymentMethod: Method of payment (e.g., "Visa")
DiscountedTransaction
Contains the original transaction plus discount calculations:

transaction: The original transaction
appliedDiscounts: List of all applicable discounts with their names and values
finalDiscount: Final calculated discount after applying combination rules
finalPrice: Final price after discount is applied
LogEntry
Used for system logging and operation tracking:

timestamp: When the log entry was created
logLevel: Severity level of the log (INFO, ERROR, etc.)
message: Log message content
Discount Rules
The system implements several discount types, each with specific eligibility criteria:

Discount Type	Description	Value	Conditions
ExpiryDateDiscount	Based on product's expiration date	1% per day for last 30 days	Products within 30 days of expiration
CheeseDiscount	Fixed discount for cheese products	10%	Product name contains "cheese"
WineDiscount	Fixed discount for wine products	5%	Product name contains "wine"
SpecialDayDiscount	Promotional discount on specific date	50%	Transaction date is March 23rd
QuantityDiscount	Tiered discount based on quantity purchased	5-10%	Purchase quantity 6 or more items
AppDiscount	Discount for mobile application purchases	5-20%	Channel is "App"
VisaDiscount	Discount for using Visa payment method	5%	Payment method is "Visa"
Discount Calculation Logic
Multiple Discounts: When multiple discounts apply, the final discount is calculated as the average of the top two highest applicable discounts.
Mutually Exclusive Discounts: Quantity and App discounts are mutually exclusive - only the higher discount will apply.
Final Price Calculation: The final price is calculated by applying the final discount percentage to the original unit price.
Data Flow
The application follows this processing sequence:

Read and parse CSV file containing transaction data
For each valid transaction:
Apply discount rules to calculate applicable discounts
Determine final discount and price
Save the discounted transaction to the database
Log the process at each stage
Configuration
The engine uses the following configuration parameters:

csvFilePath: Path to the input CSV file
logFilePath: Path to the log file
dbUrl: JDBC URL for the PostgreSQL database connection
These values are currently hardcoded in the main application object.

Installation and Setup
Prerequisites
JDK 8 or higher
Scala 2.12 or higher
PostgreSQL 9.6 or higher
SBT (Scala Build Tool)
Database Setup
The application requires a PostgreSQL database with the following table:

sql
CREATE TABLE discounted_transactions (
  id SERIAL PRIMARY KEY,
  timestamp DATE NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  expiry_date DATE NOT NULL,
  quantity INT NOT NULL,
  unit_price FLOAT NOT NULL,
  channel VARCHAR(50) NOT NULL,
  payment_method VARCHAR(50) NOT NULL,
  discount DOUBLE PRECISION NOT NULL,
  final_price DOUBLE PRECISION NOT NULL,
  applied_discounts VARCHAR(255)
);
Building the Application
bash
# Clone the repository
git clone https://github.com/your-organization/discount-rule-engine.git
cd discount-rule-engine

# Build with SBT
sbt clean compile
sbt assembly
Usage Instructions
Input CSV Format
The input CSV file must have the following header and corresponding data:

timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method
2023-01-01,Aged Cheddar Cheese,2023-02-15,5,12.99,Store,Cash
2023-01-02,Red Wine,2023-06-30,2,24.50,App,Visa
...
Running the Application
bash
# Run using SBT
sbt run

# Alternatively, run the JAR directly
java -jar target/scala-2.12/discount-rule-engine-assembly-1.0.jar
Monitoring Execution
The application logs its operation to the specified log file. You can monitor the progress using:

bash
tail -f rules_engine.log
Error Handling
The application uses Scala's Either type for functional error handling. Errors are properly logged with:

Error message
Stack trace (for critical errors)
Context information
Common error scenarios include:

Failed CSV parsing
Database connection issues
Transaction processing errors
Extending the System
Adding New Discount Rules
To add a new discount rule:

Create a new function in the DiscountRules object with the signature:
scala
val newDiscount: Transaction => Option[(String, Double)] = transaction => {
  // Your discount logic here
  if (conditionMet) Some(("NewDiscountName", discountValue))
  else None
}
Add the new discount to the getApplicableDiscounts function:
scala
val getApplicableDiscounts: Transaction => List[(String, Double)] = transaction => {
  List(
    // Existing discounts
    newDiscount(transaction),
    // Other discounts
  ).flatten
}
Modifying Discount Combination Rules
The current implementation uses the average of the top two discounts. To change this logic, modify the calculateFinalDiscount function in the DiscountRules object.

Troubleshooting
Common Issues
CSV Parsing Errors
Check that your CSV format matches the expected header
Verify date formats are YYYY-MM-DD
Ensure numeric fields contain valid numbers
Database Connection Issues
Verify PostgreSQL is running
Check database credentials
Ensure the database schema is properly set up
Discount Not Applied
Review discount rule conditions
Check transaction data meets the criteria for discounts
Log File Analysis
The log file contains detailed information about application execution. Key log messages include:

"Starting transaction processing"
"Parsed X transactions. Successful: Y, Failed: Z"
"Applied discount of X% to ProductName"
Error messages for failed operations
API Reference
Core Functions
DiscountRules.applyDiscounts
scala
val applyDiscounts: Transaction => DiscountedTransaction
Applies all discount rules to a transaction and returns a DiscountedTransaction with discount calculations.

DataTransformation.readAndParseCSVFile
scala
def readAndParseCSVFile(filePath: String): Either[Throwable, List[Either[Throwable, Transaction]]]
Reads a CSV file from the given path and parses it into a list of transactions or errors.

IOOperations.saveTransaction
scala
def saveTransaction(connection: Connection, transaction: DiscountedTransaction): Either[Throwable, Unit]
Saves a discounted transaction to the database.

Key Helper Functions
DiscountRules.calculateFinalDiscount
scala
val calculateFinalDiscount: List[(String, Double)] => Double
Calculates the final discount from a list of applicable discounts.

IOOperations.writeLogEntry
scala
def writeLogEntry(entry: LogEntry, filePath: String): Either[Throwable, Unit]
Writes a log entry to the specified file.

Note: This documentation describes the current state of the Discount Rule Engine. Future versions may include additional features and improvements.

