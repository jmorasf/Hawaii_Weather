from flask import Flask, jsonify, g
import pandas as pd
from pandas import DataFrame

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#Common steps to connect to the database
@app.before_request
def database_connection():

    # Need to get query then use date a key and prcp as the value

    import pandas as pd
    from pandas import DataFrame

    import sqlalchemy
    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine, func
    import datetime as dt

    engine = create_engine("sqlite:///Resources/hawaii.sqlite")

    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)

    g.Measurement = Base.classes.measurement
    g.Station = Base.classes.station

    g.session = Session(engine)
    
    # get last date
    last_data_point = g.session.query(g.Measurement.date).order_by(g.Measurement.date.desc()).first()
    g.last_date = last_data_point[0]
    last_date_dt = dt.datetime.strptime(g.last_date,'%Y-%m-%d')
    
    # get last month/day in the data
    g.last_mmdd = g.last_date[5:10]
    
    # get the date one year before the last date 
    year_before_dt = dt.date(last_date_dt.year-1,last_date_dt.month,last_date_dt.day)
    g.year_before = dt.datetime.strftime(year_before_dt,'%Y-%m-%d')
    
    return

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'session'):
        g.session.close()
        
#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to the Hawaii Weather API!<br/><br/>"
        f"&bull; Dates must be provided in the YYYY-MM-DD format<br/>"
        f"&bull; Start date must be before 2017-08-23<br/><br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;</br>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;</br>"
    )

@app.route("/api/v1.0/precipitation")  
def precipitation():
    """Return the query results for last year of precipitation"""
    
    # Set query to get average values
    results_avg = g.session.query(
    g.Measurement.date,
    func.avg(g.Measurement.prcp).label('Average Precipitation')
        ).group_by(g.Measurement.date
        ).filter(g.Measurement.date > g.year_before
        ).order_by(g.Measurement.date).all()

    df_avg = DataFrame(results_avg)


    df_avg.set_index('date',inplace = True)
    df_avg.sort_index(inplace=True)
    dict = df_avg.to_dict('index')
  
    return jsonify(dict)

@app.route("/api/v1.0/tobs")  
def tobs():
    """Return the query results for last year of average temperature measurements"""
    
    results_avg = g.session.query(
    g.Measurement.date,
    func.avg(g.Measurement.tobs).label('Average Temperature')
        ).group_by(g.Measurement.date
        ).filter(g.Measurement.date > g.year_before
        ).order_by(g.Measurement.date).all()

    df_avg = DataFrame(results_avg)


    df_avg.set_index('date',inplace = True)
    df_avg.sort_index(inplace=True)
    dict = df_avg.to_dict('index')
  
    return jsonify(dict)

@app.route("/api/v1.0/stations")  
def station_list():
    """Return the list of weather stations as json"""

    stations = g.session.query(g.Station.station,g.Station.name,g.Station.latitude,g.Station.longitude,g.Station.elevation)
    df = DataFrame(stations)

    df.set_index('station',inplace = True)
    df.sort_index(inplace=True)
    dict = df.to_dict('index')
    print (type(dict))
  
    return jsonify(dict)

@app.route("/api/v1.0/<start>")
def temperature_range_by_start_date(start):
    """Return the list of weather measurements for the last year of data
       starting on the month/day of the <start> date provided, or a 404 if not."""
    
    def calc_temps(start_date):
    
        return g.session.query(func.min(g.Measurement.tobs), func.avg(g.Measurement.tobs), func.max(g.Measurement.tobs)).\
            filter(g.Measurement.date >= start_date).all()
   
    tmin,tave,tmax = calc_temps(start)[0]

    dict = {"Lowest Temp" : tmin,
	    "Average Temp" : tave,
	    "Highest Temp" : tmax,
            }

    return jsonify(dict)
    return jsonify({"error": "Invalid date format. Please use 'YYYY-MM-DD' date format."}), 404

@app.route("/api/v1.0/<start>/<end>")
def temperature_range_by_date(start, end):
    """Return the list of weather measurements for the last year of data
       using the corresponding date interval provided with the <start><end> arguments, or a 404 if not."""
    
    def calc_temps(start_date, end_date):
    
        return g.session.query(func.min(g.Measurement.tobs), func.avg(g.Measurement.tobs), func.max(g.Measurement.tobs)).\
            filter(g.Measurement.date >= start_date).filter(g.Measurement.date <= end_date).all()

    tmin,tave,tmax = calc_temps(start, end)[0]

    dict = {"Lowest Temp" : tmin,
	    "Average Temp" : tave,
	    "Highest Temp" : tmax,
            }

    return jsonify(dict)
    return jsonify({"error": "Invalid date format. Please use 'YYYY-MM-DD' date format."}), 404

if __name__ == "__main__":
    app.run(debug=True)

