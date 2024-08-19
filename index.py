from dash import Dash, html, dcc, Input,Output, callback,ctx, State,DiskcacheManager, CeleryManager
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import geopandas as gpd
import property
from google.cloud import bigquery

import ast
import AI_langchain
from AI_langchain import sql_chain, full_chain, check_memory, run_query

import os
import time

#Data 
from data_import import Query1, Query2,Query3,Query4,Query5


df_city= run_query(Query1)
df_category= run_query(Query2)
df_city_store= run_query(Query3)
df_city_store= df_city_store.dropna()
df_county= run_query(Query4)
df_county_cities= run_query(Query5)


#Checking os variables 
if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    from celery import Celery
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)

else:
    # Diskcache for non-production apps when developing locally
    import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)


#Read map data 
# Load and prepare geo-data
counties = gpd.read_file("./cb_2018_us_county_500k/")
counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
counties= counties[counties['STATEFP']=='19']
counties = counties.set_index("GEOID")
counties['NAME']= counties['NAME'].str.upper()

#Sales by County

df_county['county']= df_county['county'].str.upper()
df_sales_counties = pd.merge(counties, df_county, left_on='NAME',right_on= 'county', how='inner')

app = Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}],
            background_callback_manager=background_callback_manager)

app.layout = html.Div((

    html.Div([
        html.Div([
            html.Div([
                html.H3('Iowa Liquor Sales Insights', style = {'margin-bottom': '0px', 'color': 'white'}),
            ])
        ], className = "one third column", id = "title1"),

        html.Div([
            html.P('Year', className = 'fix_label', style = {'color': 'white'}),
            dcc.Slider(id = 'select_year',
                       included = False,
                       updatemode = 'drag',
                       tooltip = {'always_visible': True},
                       min = 2012,
                       max = 2024,
                       step = 1,
                       value = 2018,
                       marks = {str(yr): str(yr) for yr in range(2012, 2025)},
                       className = 'dcc_compon'),

        ], className = "one-half column", id = "title2"),

        html.Div([
            html.P('Analyzing Trends and Performance', className = 'fix_label', style = {'color': 'white'}),
            dcc.RadioItems(id = 'radio_items',
                           labelStyle = {"display": "inline-block"},
                           value = 'Year',
                           options = [{'label': i, 'value': i} for i in ['Year','County']],
                           style = {'text-align': 'center', 'color': 'white'}, className = 'dcc_compon'),

        ], className = "one-third column", id = 'title3'),

    ], id = "header", className = "row flex-display", style = {"margin-bottom": "25px"}),

    html.Div([
        html.Div([
            dcc.RadioItems(id = 'radio_items1',
                           labelStyle = {"display": "inline-block"},
                           value = 'city',
                           options = [{'label': 'Category', 'value': 'category_name'},
                                      {'label': 'city', 'value': 'city'}],
                           style = {'text-align': 'center', 'color': 'white'}, className = 'dcc_compon'),
            dcc.Graph(id = 'bar_chart1',
                      #clickData={'points':[{'label':'AMERICAN VODKAS'}]},
                      config = {'displayModeBar': 'hover'}, style = {'height': '350px', 'width':'350px'}),

        ], className = 'create_container2 three columns', style = {'height': '400px'}),

        html.Div([
            dcc.Graph(id = 'donut_chart',
                      config = {'displayModeBar': 'hover'}, style = {'height': '350px'}),

        ], className = 'create_container2 three columns', style = {'height': '400px'}),

        html.Div([
            dcc.Graph(id = 'line_chart',
                      config = {'displayModeBar': 'hover'}, style = {'height': '350px'}),

        ], className = 'create_container2 four columns', style = {'height': '400px'}),


        html.Div([
              html.Div(id='text1'),
              html.Div(id='text2'),
              html.Div(id='text3'),

         ], className = 'create_container2 two columns', style = {'width': '260px'}),

    ], className = "row flex-display"),

    html.Div((
      
        html.Div([
            dcc.RadioItems(id = 'radio_items2',
                           labelStyle = {"display": "inline-block"},
                           value = 'County',
                           options = [{'label': 'County', 'value': 'County'},
                                      {'label': 'City', 'value': 'City'}],
                           style = {'text-align': 'center', 'color': 'white'}, className = 'dcc_compon'),
            dcc.Graph(id = 'bar_chart3',
                      config = {'displayModeBar': 'hover'}),

        ], className = 'create_container2 three columns'),

        html.Div([
            html.Div([
            html.Div([
            dcc.Dropdown(
    options=  [{'label': i, 'value': i} for i in counties['NAME'].tolist()],
    value= None, id='bubble_dropdown', optionHeight=50, 
    placeholder="Select County",
                    )], style = {'width': '49%', 'display': 'inline-block', 'textAlign': 'left'}),
            html.Div(id='bubble_chart_title',style = {'width': '49%', 'display': 'inline-block'}), 
                ],style={"display": "flex", 'text-align': 'center', 'color': 'white'}),

            dcc.Graph(id = 'bubble_chart',
                       #clickData={'points':[{'customdata':None}]},
                      config = {'displayModeBar': 'hover'}),

        ], className = 'create_container2 six columns'),
          html.Div([

    html.Div([
        html.Div( html.Div( "Ask a question about the Iowa Liquor Sales data (e.g., 'Show me the top 10 products "
    "sold in Des Moines last year'). Please note that the query can only fetch up to 2GB of data. "
    "Example questions: 'What were the top 5 counties by total sales revenue in 2023?' "
    "or 'How many liquor bottles were sold in Polk County in January 2024?'", 
             style={
                 'color': '#888',
                 'font-style': 'italic',
                 'text-align': 'center',
                 'padding': '20px'
             }),
            id='display-conversation', 
                 style={"height": "calc(40vh - 12px)", 
                        'overflow-y': 'auto', 
                         "display": "flex",
                        'border': '1px solid #ccc', 
                        'color': 'white',
                        'padding': '10px'}),
        html.Br(),
        html.Div([
            html.Div(className='spinner'),
            html.P('Loading...', style={'color':'white'})

        ], className= 'loading-indicator', 
        id='progress-spinner', style={"visibility": "hidden"}),
                     html.Div(
            className="chat-input-container",
            children=[
                dcc.Input(
                    id="chatInput",
                    type="text",
                    placeholder="Type your question...",
                    className="chat-input"
                ),
                html.Button(
                    "Submit",
                    id="submit",
                    disabled=True,  # Initially disabled
                    className="send-button"
                )
            ]
        ),
       

    ], style={ 'width': '100%', 'max-width': '600px', 'margin': 'auto'})


        ], className = 'create_container2 three columns'),

    ), className = "row flex-display"),

    

), id= "mainContainer", style={"display": "flex", "flex-direction": "column"})


def create_bar(dff,radio_items1, select_year):
        y= dff[radio_items1] #if radio_items1=='city' else dff['category_name']
        x= dff['total_sales']
        year= dff['year']
        sales_by= "Category" if radio_items1== 'category_name' else 'City'
        return {
        'data':[go.Bar(
                    x=x,
                    y=y,
                    text =x,
                    texttemplate = '$' + '%{text:.2s}',
                    textposition = 'auto',
                    orientation = 'h',
                    marker = dict(color='#19AAE1 '),

                    hoverinfo='text',
                    hovertext=
                    '<b>Year</b>: ' + year.astype(str) + '<br>' +
                    # '<b>Segment</b>: ' + y.astype(str) + '<br>' +
                    # '<b>Sub-Category</b>: ' + y.astype(str) + '<br>' +
                    '<b>Sales</b>: $' + [f'{x:,.2f}' for x in x] + '<br>'



              )],


        'layout': go.Layout(
             plot_bgcolor='#1f2c56',
             paper_bgcolor='#1f2c56',
             title={
                'text': 'Sales by ' + str((sales_by))+ ' in year' + ' ' + str((select_year)),

                'y': 0.99,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
             titlefont={
                        'color': 'white',
                        'size': 12},

             hovermode='closest',
             margin = dict(t = 40, r = 0, l=150),

             xaxis=dict(title='<b></b>',
                        color = 'orange',
                        showline = True,
                        showgrid = True,
                        showticklabels = True,
                        linecolor = 'orange',
                        linewidth = 1,
                        ticks = 'outside',
                        tickfont = dict(
                            family = 'Arial',
                            size = 12,
                            color = 'orange ')


                ),

             yaxis=dict(title='<b></b>',
                        autorange = 'reversed',
                        color = 'orange ',
                        showline = False,
                        showgrid = False,
                        showticklabels = True,
                        linecolor = 'orange',
                        linewidth = 1,
                        ticks = 'outside',
                        tickfont = dict(
                            family = 'Arial',
                            size = 12,
                            color = 'orange')

                ),

            legend = {
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'x': 0.5,
                'y': 1.25,
                'xanchor': 'center',
                'yanchor': 'top'},

            font = dict(
                family = "sans-serif",
                size = 15,
                color = 'white'),


                 )

    }


#####Time series ###
def create_time_series(dff, radio_items1, select_name):
    x= dff['year']
    y= dff['total_sales']
    sales_by= "Category" if radio_items1== 'category_name' else 'City'

    return {
        'data':[
            go.Scatter(
                x = x,
                y = y,
                name = 'Sales',
                text = y,
                texttemplate = '%{text:.2s}',
                textposition = 'bottom left',
                mode = 'markers+lines+text',
                line = dict(width = 3, color = 'orange'),
                marker = dict(size = 10, symbol = 'circle', color = '#19AAE1',
                              line = dict(color = '#19AAE1', width = 2)
                              ),

                hoverinfo = 'text',
                hovertext =
                '<b>Year</b>: ' + x.astype(str) + '<br>' +
                '<b>Sales</b>: $' + [f'{x:,.2f}' for x in y] + '<br>'

            )],


        'layout': go.Layout(
             plot_bgcolor='#1f2c56',
             paper_bgcolor='#1f2c56',
             title={
                'text': f'Sales Trend by  {sales_by}  ({select_name})' ,

                'y': 0.99,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
             titlefont={
                        'color': 'white',
                        'size': 15},

             hovermode='closest',
             margin = dict(t = 5, l = 0, r = 0),

             xaxis = dict(title = '<b></b>',
                          visible = True,
                          color = 'orange',
                          showline = True,
                          showgrid = False,
                          showticklabels = True,
                          linecolor = 'orange',
                          linewidth = 1,
                          ticks = 'outside',
                          tickfont = dict(
                             family = 'Arial',
                             size = 12,
                             color = 'orange')

                         ),

             yaxis = dict(title = '<b></b>',
                          visible = True,
                          color = 'orange',
                          showline = False,
                          showgrid = True,
                          showticklabels = False,
                          linecolor = 'orange',
                          linewidth = 1,
                          ticks = '',
                          tickfont = dict(
                             family = 'Arial',
                             size = 12,
                             color = 'orange')

                         ),

            legend = {
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'x': 0.5,
                'y': 1.25,
                'xanchor': 'center',
                'yanchor': 'top'},

            font = dict(
                family = "sans-serif",
                size = 12,
                color = 'white'),

        )

    }



@callback(
    Output('bar_chart1', 'figure'),
    Input('select_year', 'value'),
    Input('radio_items1', 'value'),
)
def update_bargraph1(select_year,radio_items1):

    if radio_items1== 'city':
        dff= df_city.groupby(['year','city'])['total_sales'].sum().reset_index()
    else:
        dff= df_category.groupby(['year','category_name'])['total_sales'].sum().reset_index()
        
    dff= dff[dff['year']== select_year]
    dff= dff.sort_values(by = ['total_sales'], ascending = False).nlargest(5, columns = ['total_sales'])

    return create_bar(dff,radio_items1, select_year)

@callback(
          
     Output('line_chart', 'figure'),
     Output('bar_chart1','clickData'),
     Input('bar_chart1','clickData'),
     Input('radio_items1', 'value'),
     Input('select_year', 'value'),
)
def update_timeseries(clickData, radio_items1,select_year):
    
    
    group_var= 'category_name' if radio_items1=='category_name' else 'city'

    if group_var=='city':
        if clickData:
            select_name= clickData['points'][0]['label']
        else:
            df_year_x = df_city[df_city['year'] == select_year]
            select_name = df_year_x.loc[df_year_x['total_sales'].idxmax(), 'city']
        dff= df_city[df_city[group_var]== select_name]
        
    else:
        if clickData:
            select_name= clickData['points'][0]['label']
        else:
            df_year_x = df_category[df_category['year'] == select_year]
            select_name = df_year_x.loc[df_year_x['total_sales'].idxmax(), 'category_name']
        dff= df_category[df_category[group_var]== select_name]
        

    dff= dff.groupby(['year', group_var])['total_sales'].sum().reset_index()
    

    tl= 'Category' if radio_items1=='category_name' else 'City'
    title='<b>{}</b><br>{}'.format(tl, select_name)
    return create_time_series(dff, radio_items1, select_name),None


@callback(
    Output('donut_chart','figure'),
    Input('select_year', 'value')
)
def update_donut(select_year):
    dff= df_city_store.groupby(['year', 'city'])['total_sales'].sum().reset_index()
    dff= dff[dff['year']== select_year]
    dff= dff.sort_values(by = ['total_sales'], ascending = False).nlargest(5, columns = ['total_sales'])
    dff2= df_city_store[(df_city_store['city'].isin(dff['city'])) &(df_city_store['year']==select_year) ]
    colors = ['#30C9C7', '#7A45D1', '#FFC300', '#ff33e0','#36ff33']
   
    def extract_first_two_words(text):
        words = text.split()
        return ' '.join(words[:2])
    
    dff2['labels_name']= [extract_first_two_words(text) for text in dff2['store_name'].tolist()]
    fig = px.sunburst(dff2, path=['city', 'labels_name'], values='total_sales',
                      hover_data=['city','labels_name'])
    fig.update_traces(hovertemplate = 'City: ' "<b>%{customdata[0]}</b><br>"+
                                       'Store:' "<b>%{customdata[1]}</b><br>"+
                                       'Sales:' "<b>%{value}</b><br>")
    fig.update_layout(
                      margin = dict(t=50, l=0, r=0, b=0),
                      plot_bgcolor = '#1f2c56',
                      paper_bgcolor = '#1f2c56',
                      hovermode = 'x',
                               title = {
                'text': 'Sales by Store in Year' + ' ' + str((select_year)),

                'y': 1,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont = {
                'color': 'white',
                'size': 15},
            legend = {
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'xanchor': 'center', 'x': 0.5, 'y': -0.15},
            font = dict(
                family = "sans-serif",
                size = 12,
                color = 'white')
        )
    return fig


@callback(
   Output('bubble_chart', 'figure'),
   Input('select_year', 'value'),
   Input('bubble_dropdown', 'value')

)
def update_bubble_chart(select_year, bubble_dropdown):
    
    if not bubble_dropdown:
        dff4= df_sales_counties[df_sales_counties['year']==select_year]
    else:
        dff4= df_sales_counties[(df_sales_counties['year']==select_year) & (df_sales_counties['county']== bubble_dropdown)]

    

    fig = px.choropleth(dff4, geojson=dff4.geometry, locations=dff4.index, color=dff4['total_sales'],
                        color_continuous_scale=property.color_continuous_scale,
                         hover_data=['county','total_sales'] )
    fig.update_geos(fitbounds="locations", visible=False)
    # fig.update_traces(hovertemplate = 'County: ' "<b>%{customdata[0]}</b><br>"+
    #                                    'Sales:' "<b>%{customdata[0]}</b><br>")
    fig.update_layout(
                      margin = dict(t=0, l=0, r=0, b=0),
                      plot_bgcolor = '#1f2c56',
                      paper_bgcolor = '#1f2c56',
                      hovermode = 'x',
                        
            titlefont = {
                'color': 'white',
                'size': 15},
            legend = {
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'xanchor': 'center', 'x': 0.5, 'y': -0.15},
            font = dict(
                family = "sans-serif",
                size = 12,
                color = 'white')
        )

    return fig

@callback(
    Output('bubble_chart_title', 'children'),
    Input('select_year','value'),
    Input('bubble_dropdown', 'value')
)
def update_bubble_title(select_year, bubble_dropdown):
    county= f'({bubble_dropdown})' if bubble_dropdown else ""
    return f'Sales by County in {select_year}  {county}'

@callback(
    Output('bubble_dropdown', 'value'),
    Output('bubble_chart', 'clickData'),
    Input('bubble_chart', 'clickData')
)
def update_dropdown(clickData):
    if clickData is None:
        return None,None
    return clickData['points'][0]['customdata'][0],None


# Callback to enable/disable the button based on input value
@app.callback(
    Output("submit", "disabled"),
    [Input("chatInput", "value")]
)
def set_button_state(value):
    if value and value.strip():
        return False  # Enable the button
    return True  # Disable the button if the input is empty

#Call the chart bot 
@app.callback(
    Output("display-conversation", "children"),# Output("loading-component", "children")],
    Output("chatInput", "value"),
    Input("submit", "n_clicks"), 
    State("chatInput", "value"),
    #[State("user-input", "value"), State("display-conversation", "children")],
    background=True,
    running=[
        (Output("submit", "disabled"), True, False),
        (Output("chatInput", "disabled"), True, False),
        (
            Output("progress-spinner", "style"),
            {"visibility": "visible"},
            {"visibility": "hidden"},
        )
    ],
    
    prevent_initial_call=True,
   
)
def run_chatbot( n_clicks, value):
    # Simulate query and answer generation
    start_time = time.time()
    sql_response =  ast.literal_eval(sql_chain.invoke({'input': {value}}))
    generated_query = f"{sql_response['query']}"

    # if the query is more than 2 gb dont run 
   
    if sql_response['query']:
        if check_memory(sql_response['query']):
            response = full_chain.invoke({'input': value})
        else:
            response = f'The query generated will process more than 2GB of data!'
    else:
        response = full_chain.invoke({'input': value})
    end_time = time.time()
    execution_time = end_time - start_time
    #print(f"Execution time: {execution_time} seconds")
    
    return html.Div([
         html.Button(
                    "Question",
                    id="submit_query",
                    disabled=True,  # Initially disabled
                    className="send-button"
                ),
        html.P(f"**Question:** {value}", className="output-question"),
         html.Button(
                    "Query",
                    id="submit_query",
                    disabled=True,  # Initially disabled
                    className="send-button"
                ),
        html.P(f"**Generated Query:** {generated_query}", className="output-query"),
        html.Button(
                    "Answer",
                    id="submit_query",
                    disabled=True,  # Initially disabled
                    className="send-button"
                ),
        html.P(f"**Answer:** {response}", className="output-answer"),
    ]), ""

# Update bar_chart3 
@callback(
    Output('bar_chart3', 'figure'),
    Output('radio_items2','value'),
    Input('bubble_dropdown', 'value'),
    Input('select_year', 'value')

)
def update_bar3(bubble_dropdown,select_year):
    if bubble_dropdown:
        dff= df_county_cities[(df_county_cities['county']==bubble_dropdown) & (df_county_cities['year']==select_year)]
        dff= dff.dropna()
        y=dff['city']
        x=dff['total_sales']
        value="City"
        city_title= f' in County {bubble_dropdown}'
    else:
        dff= df_county[df_county['year']==select_year]
        dff= dff.dropna()
        dff= dff.sort_values(by = ['total_sales'], ascending = False).nlargest(5, columns = ['total_sales'])
        y=dff['county']
        x=dff['total_sales']
        value="County"
        city_title=" "
    

    return {
        'data':[go.Bar(
                    x=x,
                    y=y,
                    text =x,
                    texttemplate = '$' + '%{text:.2s}',
                    textposition = 'auto',
                    orientation = 'h',
                    marker = dict(color='#19AAE1 '),

                    # hoverinfo='text',
                    # hovertext=""
                    #'<b>Year</b>: ' + year.astype(str) + '<br>' +
                    # '<b>Segment</b>: ' + y.astype(str) + '<br>' +
                    # '<b>Sub-Category</b>: ' + y.astype(str) + '<br>' +
                    #'<b>Sales</b>: $' + [f'{x:,.2f}' for x in x] + '<br>'



              )],


        'layout': go.Layout(
             plot_bgcolor='#1f2c56',
             paper_bgcolor='#1f2c56',
             title={
                'text': 'Sales by ' + str((value))+ ' in year' + ' ' + str((select_year))+' '+ city_title ,

                'y': 0.99,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
             titlefont={
                        'color': 'white',
                        'size': 12},

             hovermode='closest',
             margin = dict(t = 40, r = 0, l=150),

             xaxis=dict(title='<b></b>',
                        color = 'orange',
                        showline = True,
                        showgrid = True,
                        showticklabels = True,
                        linecolor = 'orange',
                        linewidth = 1,
                        ticks = 'outside',
                        tickfont = dict(
                            family = 'Arial',
                            size = 12,
                            color = 'orange ')


                ),

             yaxis=dict(title='<b></b>',
                        autorange = 'reversed',
                        color = 'orange ',
                        showline = False,
                        showgrid = False,
                        showticklabels = True,
                        linecolor = 'orange',
                        linewidth = 1,
                        ticks = 'outside',
                        tickfont = dict(
                            family = 'Arial',
                            size = 12,
                            color = 'orange')

                ),

            legend = {
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'x': 0.5,
                'y': 1.25,
                'xanchor': 'center',
                'yanchor': 'top'},

            font = dict(
                family = "sans-serif",
                size = 15,
                color = 'white'),


                 )

    }, value
 
server = app.server

if __name__ == '__main__':
    app.run_server(debug=False)
