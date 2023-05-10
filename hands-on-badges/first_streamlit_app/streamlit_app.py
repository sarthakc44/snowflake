import streamlit, pandas, requests, snowflake.connector
from urllib.error import URLError 

streamlit.title('My Parents New Healthy Diner')

streamlit.header('Breakfast Favorites')
streamlit.text('🥣 Omega 3 & Blueberry Oatmeal')
streamlit.text('🥗Kale, Spinach & Rocket Smoothie')
streamlit.text('🐔 Hard-Boiled Free-Range Egg')
streamlit.text('🥑🍞 Avocado Toast')

streamlit.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')
my_fruit_list = pandas.read_csv("https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt")
my_fruit_list = my_fruit_list.set_index('Fruit')

# Let's put a pick list here so they can pick the fruit they want to include 
fruits_selected = streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index),['Avocado','Strawberries'])

# Display the table on the page.
fruits_to_show = my_fruit_list.loc[fruits_selected]
streamlit.dataframe(fruits_to_show)

def get_fruityvice_data(this_fruit_choice):
    fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+ this_fruit_choice)
    # normalize the json response
    fruityvice_normalized = pandas.json_normalize(fruityvice_response.json())
    
    return fruityvice_normalized


#new section to display fruityvice api response
streamlit.header("Fruityvice Fruit Advice!")
try:
  # input textbox with default value as Kiwi
  fruit_choice = streamlit.text_input('What fruit would you like information about?')
  # streamlit.text(fruityvice_response.json()) #only json
  if not fruit_choice:
    streamlit.error("Please enter a fruit to get information!")
  else:
    fruityvice_normalized =  get_fruityvice_data(fruit_choice)
    # output normalized response as df
    streamlit.dataframe(fruityvice_normalized)

except URLError as e:
  streamlit.error()
    

streamlit.header("The fruit load list contains:")

def get_fruit_load_list():
    with my_cnx.cursor() as my_cur:
        my_cur.execute("SELECT * from fruit_load_list")
        return my_cur.fetchall()

#button to load fruit list

if streamlit.button('Get Fruit Load List'):
    my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    my_data_rows = get_fruit_load_list()
    my_cnx.close()
    streamlit.dataframe(my_data_rows)

def insert_row_snowflake(new_fruit):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("insert into fruit_load_list values ('" +new_fruit+ "' )")
        return "Thanks for adding " + new_fruit

# input textbox to add fruit to fruit_load_list
add_my_fruit = streamlit.text_input('What fruit would you like to add?')
if streamlit.button('Add a new fruit to the list!'):
    my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    new_fruit = insert_row_snowflake(add_my_fruit)
    my_cnx.close()
    streamlit.text(new_fruit)

