from config import API_key
import requests
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.orm import join
from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float



# #################################################################
# # DATA RETRIEVAL AND MUNGING FOR AIRPORT DETAILS
# #################################################################

AirportCodes = ["ATL", "BOS", "BWI","CLT","DAL", "DCA", "DEN", "EWR", "FLL", "JFK", "LAS", 
"LAX", "LGA", "MCO", "MDW", "MIA", "MSP", "ORD", "PDX", "PHL", "PHX", "SAN", "SFO", "SLC", "TPA"]

AirportAddress =[]
Lat = []
Lng = []
AirportName = []
counter = 0


##### Gathering the Airport address , lat and Lng values of the airports #########
for airport_code in AirportCodes: 

    URL = f"https://maps.googleapis.com/maps/api/geocode/json?address={airport_code}&key={API_key}"

    results = requests.get(URL).json()

    formatted_address = results["results"][0]["formatted_address"]
    lat = results["results"][0]["geometry"]["location"]["lat"]
    lng = results["results"][0]["geometry"]["location"]["lng"]

    short_name = results["results"][0]["address_components"][0]["short_name"]
    airportname = short_name.split(" Airport")[0]  ## removing the Airport from the name
    
     
    # Inserting the gathered values to their respective Lists.
    AirportAddress.append(formatted_address)
    Lat.append(lat)
    Lng.append(lng)
    AirportName.append(airportname)
#     # print (f"{counter} : Airport Name : {airportname} --- Airport Code:{airport_code}")
    
### Insert the list to a DataFrame before inserting to SQL

AirportDetailsDF = pd.DataFrame({"AIRPORT_NAME" : AirportName,
                                 "AIRPORT_CODE" : AirportCodes,
                                "AIRPORT_ADDRESS" : AirportAddress,
                                "Lat" :Lat, 
                                "Lng" :Lng
                                })


######## Finding the Airline Codes only for the selected Airlines list below  ###############################

Airlines = ["Hawaiian Airlines Inc.",
"Delta Air Lines Inc.",
"United Air Lines Inc.",
"American Airlines Inc.",
"Frontier Airlines Inc.",
"Southwest Airlines Co.",
"JetBlue Airways",
"ExpressJet Airlines LLC",
"SkyWest Airlines Inc.",
"Alaska Airlines Inc."]

EntireCarrierDF = pd.read_csv("../resources/L_CARRIERS.csv")
EntireCarrierDF.set_index("Code")

CarrierCode = []

for airline in Airlines: 
    code = list(EntireCarrierDF.loc[EntireCarrierDF["Description"]== airline, "Code"])
    CarrierCode.append(code[0])

CarrierCodeDF = pd.DataFrame({"Code":CarrierCode})

FinalCarrierCodeDF = pd.merge(EntireCarrierDF,CarrierCodeDF, how="inner", on="Code" )

#################################################################
# ORIGIN AND DESTINATION SURVEY DATA
#Airline Origin and Destination Survey (DB1B)
#################################################################

q12018 = pd.read_csv("../resources/789605885_T_DB1B_MARKET_Q1.csv")
q22018 = pd.read_csv("../resources/789605885_T_DB1B_MARKET_Q2.csv")
q32018 = pd.read_csv("../resources/789605885_T_DB1B_MARKET_Q3.csv")
q42018 = pd.read_csv("../resources/789605885_T_DB1B_MARKET_Q4.csv")

DB1Bframes = [q12018,q22018, q32018,q42018]
EntireDB1B_dataDF = pd.concat(DB1Bframes)

DB1B_dataDF = pd.merge(EntireDB1B_dataDF, FinalCarrierCodeDF, how ="inner", left_on="REPORTING_CARRIER", right_on="Code")

DB1B_dataDF_new = DB1B_dataDF.dropna(how='all', axis='columns')

# # # #######################################
# # # Inserting to the SQL Lite database 
# ##########################################

engine = create_engine("sqlite:///../db/flight1.sqlite")
Base = declarative_base()

class Airport_Details(Base):
    __tablename__ = 'airport_details'
    ID = Column(Integer, primary_key=True)
    AIRPORT_NAME = Column(String(255))
    AIRPORT_CODE = Column(String(255))
    AIRPORT_ADDRESS = Column(String(255))
    Lat = Column(String(255))
    Lng = Column(String(255))



class DB1B_Details(Base):
    __tablename__ = 'db1b_details'
    ID = Column(Integer, primary_key=True)
    QUARTER = Column(String(255))
    ORIGIN_AIRPORT_ID = Column(String(255))
    ORIGIN = Column(String(255))
    ORIGIN_COUNTRY = Column(String(255))
    ORIGIN_STATE_ABR = Column(String(255))
    ORIGIN_STATE_NM = Column(String(255))
    DEST_AIRPORT_ID = Column(String(255))
    DEST = Column(String(255))
    DEST_STATE_ABR = Column(String(255))
    DEST_STATE_NM = Column(String(255))
    REPORTING_CARRIER = Column(String(255))
    PASSENGERS = Column(String(255))
    MARKET_FARE = Column(String(255))
    MARKET_DISTANCE = Column(String(255))
    MARKET_MILES_FLOWN = Column(String(255))
    NONSTOP_MILES = Column(String(255))
    Code = Column(String(255))
    Description = Column(String(255))

Base.metadata.create_all(engine)
session = Session(bind=engine)

try:
    AirportDetailsDF.to_sql('airport_details', con=engine, if_exists="append", index= False)
    DB1B_dataDF_new.to_sql('db1b_details', con=engine, if_exists="append", index= False, chunksize=10000)
    print("Tables added successfully")
except Exception as e:
    print("Problems adding tables" + str(e))