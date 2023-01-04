import random
import pandas as pd
from bokeh.driving import count
from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure, show
from bokeh.models import DatetimeTickFormatter
from bokeh.models.widgets import Div
from bokeh.layouts import column,row
import time
from datetime import datetime
from kafka import KafkaConsumer


UPDATE_INTERVAL = 1000
ROLLOVER = 10 # Number of displayed data points


source = ColumnDataSource({"x": [], "y": []})
consumer = KafkaConsumer('clean-sensor-data', auto_offset_reset='latest',bootstrap_servers=['kafka:9092'], consumer_timeout_ms=1000)
div = Div(
    text='',
    width=120,
    height=35
)



@count()
def update(x):
    msg_value=None
    for msg in consumer:
        msg_value=msg
        break
    if msg_value is None:
        return
    
    values=eval(msg_value.value.decode("utf-8"))
    print(values)
    x=pd.to_datetime(values["measurement_timestamp"])
   
    print(x)

    div.text = "TimeStamp: "+str(x)


    y = values['water_temperature']
    print(y)
    
    source.stream({"x": [x], "y": [y]},ROLLOVER)

p = figure(title="Water Temperature Sensor Data",x_axis_type = "datetime",width=1000)
p.line("x", "y", source=source)

p.xaxis.formatter=DatetimeTickFormatter(hourmin = ['%H:%M'])
p.xaxis.axis_label = 'Time'
p.yaxis.axis_label = 'Value'
p.title.align = "right"
p.title.text_color = "orange"
p.title.text_font_size = "25px"

doc = curdoc()
#doc.add_root(p)

doc.add_root(
    row(children=[div,p])
)
doc.add_periodic_callback(update, UPDATE_INTERVAL)