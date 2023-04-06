import os
import openai
import logging
import json

class ChatGPT:

    startMessageStack = [
        # Introduce the role of the assistant
        {"role": "system", "content": ('You act as the middleman between USER and a DATABASE. Your main goal is to '
                                       'answer questions based on data in a SQL Server 2019 database (SERVER). You do '
                                       'this by executing valid queries against the database and interpreting the results '
                                       'to answer the questions from the USER.')},

        # Set the format for responses
        {"role": "user", "content": ('From now you will only ever respond with JSON. When you want to address the user, '
                                      'you use the following format {"recipient": "USER", "message":"message for the '
                                      'user"}.')},
        {"role": "assistant", "content": '{"recipient": "USER", "message":"I understand."}'},

        # Set the format for interacting with the SQL Server
        {"role": "user", "content": ('You can address the SQL Server by using the SERVER recipient. When calling the '
                                      'server, you must also specify an action. The action can be QUERY when you want to '
                                      'QUERY the database, or SCHEMA when you need SCHEMA information for a comma '
                                      'separated list of tables. The format you will use for requesting schema '
                                      'information is as follows {"recipient":"SERVER", "action":"SCHEMA", '
                                      '"message":"Person.Person, Person.Address"}. The format you will use for executing '
                                      'a query is as follows: {"recipient":"SERVER", "action":"QUERY", '
                                      '"message":"SELECT SUM(OrderQty) FROM Sales.SalesOrderDetail;"}')},

        # Provide a list of available tables in the database
        {"role": "user", "content": ('The following tables are available in the database: Sales.SalesOrderDetail, '
                                      'Sales.SalesOrderHeader, Person.Address, Person.Person, Production.Product, '
                                      'Production.ProductCategory, Production.ProductSubcategory. You will always first '
                                      'request the SCHEMA for a table before using the table in a QUERY.')},

        # Example conversation
        {"role": "user", "content": "Let's start! How many Orders did we ship to Bellflower?"},
        {"role": "assistant", "content": '{"recipient":"SERVER", "action":"SCHEMA", "message":"Person.Address, Sales.SalesOrderHeader"}'},
        {"role": "user", "content": ('Table, Column, DataType\nPerson.Address, AddressID, int\nPerson.Address, '
                                      'AddressLine1, nvarchar\nPerson.Address, AddressLine2, nvarchar\nPerson.Address, '
                                      'City, nvarchar\nPerson.Address, StateProvinceID, int\nPerson.Address, '
                                      'PostalCode, nvarchar\nPerson.Address, SpatialLocation, geography\nPerson.Address, '
                                      'rowguid, uniqueidentifier\nPerson.Address, ModifiedDate, datetime\n'
                                      'Sales.SalesOrderHeader, SalesOrderID, int\nSales.SalesOrderHeader, '
                                      'RevisionNumber, tinyint\nSales.SalesOrderHeader, OrderDate, datetime\n'
                                      'Sales.SalesOrderHeader, DueDate, datetime\nSales.SalesOrderHeader, ShipDate, '
                                      'datetime\nSales.SalesOrderHeader, Status, tinyint\nSales.SalesOrderHeader, '
                                      'OnlineOrderFlag, bit\nSales.SalesOrderHeader, SalesOrderNumber, nvarchar\n'
                                      'Sales.SalesOrderHeader, PurchaseOrderNumber, nvarchar\nSales.SalesOrderHeader,'
                                      'AccountNumber, nvarchar\nSales.SalesOrderHeader, CustomerID, int\n'
                                      'Sales.SalesOrderHeader, SalesPersonID, int\nSales.SalesOrderHeader, '
                                      'TerritoryID, int\nSales.SalesOrderHeader, BillToAddressID, int\n'
                                      'Sales.SalesOrderHeader, ShipToAddressID, int\nSales.SalesOrderHeader, '
                                      'ShipMethodID, int\nSales.SalesOrderHeader, CreditCardID, int\n'
                                      'Sales.SalesOrderHeader, CreditCardApprovalCode, varchar\n'
                                      'Sales.SalesOrderHeader, CurrencyRateID, int\nSales.SalesOrderHeader, '
                                      'SubTotal, money\nSales.SalesOrderHeader, TaxAmt, money\n'
                                      'Sales.SalesOrderHeader, Freight, money\nSales.SalesOrderHeader, TotalDue, '
                                      'money\nSales.SalesOrderHeader, Comment, nvarchar\nSales.SalesOrderHeader, '
                                      'rowguid, uniqueidentifier\nSales.SalesOrderHeader, ModifiedDate, datetime')},
        {"role": "assistant", "content": '{"recipient":"SERVER", "action":"QUERY", "message":"SELECT COUNT(*) AS COUNT FROM Sales.SalesOrderHeader sh JOIN Person.Address a ON sh.ShipToAddressID = a.AddressID  WHERE a.City = \'Bellflower\'"}'},
        {"role": "user", "content": "COUNT\n243"},
        {"role": "assistant", "content": '{"recipient": "USER", "message":"243 orders were shipped to Bellflower."}'},
        {"role": "user", "content": "excellent, thank you! Now the next question. How many orders did we ship to Milwaukee?"},
        {"role": "assistant", "content": '{"recipient":"SERVER", "action":"QUERY", "message":"SELECT COUNT(*) AS COUNT FROM Sales.SalesOrderHeader sh JOIN Person.Address a ON sh.ShipToAddressID = a.AddressID  WHERE a.City = \'Milwaukee\'"}'},
        {"role": "user", "content": "COUNT\n4"},
        {"role": "assistant", "content": '{"recipient": "USER", "message":"4 orders were shipped to Milwaukee."}'},
        {"role": "user", "content": "great, thank you! Now the next question. How many orders did we ship to Chicago?"},

        # The below messages will be generated by ChatGPT based on the above. Still keeping them in to reinforce the idea, not sure if useful(?).
        {"role": "assistant", "content": (
            '{\'recipient\':\'SERVER\', \'action\':\'QUERY\', \'message\':\'SELECT COUNT(*) AS COUNT FROM '
            'Sales.SalesOrderHeader sh JOIN Person.Address a ON sh.ShipToAddressID = a.AddressID  WHERE '
            'a.City = \\\'Chicago\\\'\'}'
        )},
        {"role": "user", "content": "COUNT\n30"},
        {"role": "assistant", "content": (
            '{\'recipient\': \'USER\', \'message\':\'30 orders were shipped to Chicago.\'}'
        )},
        {"role": "user", "content": "What was the top selling product in 2014 by quantity?"},
        {"role": "assistant", "content": (
            '{\'recipient\':\'SERVER\', \'action\':\'SCHEMA\', \'message\':\'Production.Product, '
            'Sales.SalesOrderDetail\'}'
        )},
        {"role": "user", "content": (
            "Production.Product, ProductID, int\nProduction.Product, Name, nvarchar\nProduction.Product, "
            "ProductNumber, nvarchar\nProduction.Product, MakeFlag, bit\nProduction.Product, FinishedGoodsFlag, "
            "bit\nProduction.Product, Color, nvarchar\nProduction.Product, SafetyStockLevel, smallint\nProduction.Product, "
            "ReorderPoint, smallint\nProduction.Product, StandardCost, money\nProduction.Product, ListPrice, money\nProduction.Product, "
            "Size, nvarchar\nProduction.Product, SizeUnitMeasureCode, nchar\nProduction.Product, WeightUnitMeasureCode, nchar\nProduction.Product, "
            "Weight, decimal\nProduction.Product, DaysToManufacture, int\nProduction.Product, ProductLine, nchar\nProduction.Product, "
            "Class, nchar\nProduction.Product, Style, nchar\nProduction.Product, ProductSubcategoryID, int\nProduction.Product, "
            "ProductModelID, int\nProduction.Product, SellStartDate, datetime\nProduction.Product, SellEndDate, datetime\nProduction.Product, "
            "DiscontinuedDate, datetime\nProduction.Product, rowguid, uniqueidentifier\nProduction.Product, ModifiedDate, datetime\nSales.SalesOrderDetail, "
            "SalesOrderID, int\nSales.SalesOrderDetail, SalesOrderDetailID, int\nSales.SalesOrderDetail, CarrierTrackingNumber, nvarchar\nSales.SalesOrderDetail, "
            "OrderQty, smallint\nSales.SalesOrderDetail, ProductID, int\nSales.SalesOrderDetail, SpecialOfferID, int\nSales.SalesOrderDetail, "
            "UnitPrice, money\nSales.SalesOrderDetail, UnitPriceDiscount, money\nSales.SalesOrderDetail, LineTotal, numeric\nSales.SalesOrderDetail, "
            "rowguid, uniqueidentifier\nSales.SalesOrderDetail, ModifiedDate, datetime"
        )},
        {"role": "assistant", "content": (
            '{\'recipient\':\'SERVER\', \'action\':\'QUERY\', \'message\':\'SELECT TOP 1 Production.Product.Name '
            'FROM Production.Product JOIN Sales.SalesOrderDetail ON Product.ProductID = SalesOrderDetail.ProductID '
            'JOIN Sales.SalesOrderHeader ON SalesOrderDetail.SalesOrderID = SalesOrderHeader.SalesOrderID WHERE '
            'YEAR(SalesOrderHeader.OrderDate) = 2014 GROUP BY Product.Name ORDER BY SUM(OrderQty) DESC;\'}'
        )},
        {"role": "user", "content": "Name\nWater Bottle - 30 oz."},
        {"role": "assistant", "content": (
            '{\'recipient\': \'USER\', \'message\':\'The top selling product by quantity in 2014 is the '
            'Water Bottle - 30 oz.\'}'
        )}
    ]

    def __init__(self, api_key, api_org="", model="gpt-3.5-turbo", max_tokens=None):
        openai.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.messages = self.startMessageStack.copy()

    def message(self, message, sender):
        logging.debug(message)
        if sender:
            message = json.dumps({'message': message, 'sender': sender})
        self.messages.append({"role": "user", "content": message})
        completion = openai.ChatCompletion.create(
            model=self.model,
            messages=self.messages,
            max_tokens=self.max_tokens  # Add the max_tokens parameter here
        )
        response = completion.choices[0].message.content
        logging.debug(response)
        self.messages.append({"role": "assistant", "content": response})
        return response

    def reset(self):
        self.messages = self.startMessageStack.copy()
        print('model was reset to intial state')

