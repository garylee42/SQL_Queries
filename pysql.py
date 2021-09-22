import psycopg2, csv


alarm_config_id = ""
zone_id = ""

sql_file = "/Users/gary.lee/Downloads32.csv"

#establishing the connection
conn = psycopg2.connect(
   database="valarm", user='readonly', password='Rnfyk0u7c3GyGKIPHBV4', host='valarm-v2.cluster-ro-cfatffc8qmd0.us-west-2.rds.amazonaws.com', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
master_query = '''SELECT "public"."postal_address"."postal_address_id" AS "postal_address_id", "Zone"."organization_id" AS "Zone__organization_id", "Zone"."zone_id" AS "Zone__zone_id", "Alarm Configuration"."alarm_configuration_id" AS "Alarm Configuration__alarm_configuration_id"
FROM "public"."postal_address" INNER JOIN "public"."zone" "Zone" ON "public"."postal_address"."postal_address_id" = "Zone"."postal_address_id"
LEFT JOIN "public"."alarm_configuration" "Alarm Configuration" ON "Zone"."zone_id" = "Alarm Configuration"."zone_id"
WHERE "Zone"."deleted" = FALSE
ORDER BY "Zone"."organization_id" DESC'''



postal_query = '''SELECT "public"."postal_address"."postal_address_id" AS "postal_address_id"
FROM "public"."postal_address" INNER JOIN "public"."zone" "Zone" ON "public"."postal_address"."postal_address_id" = "Zone"."postal_address_id"
LEFT JOIN "public"."alarm_configuration" "Alarm Configuration" ON "Zone"."zone_id" = "Alarm Configuration"."zone_id"
WHERE "Zone"."deleted" = FALSE
ORDER BY "Zone"."organization_id" DESC
LIMIT 1048575'''


org_query = '''SELECT "public"."postal_address"."country" AS "country", "Zone"."organization_id" AS "Zone__organization_id"
FROM "public"."postal_address" INNER JOIN "public"."zone" "Zone" ON "public"."postal_address"."postal_address_id" = "Zone"."postal_address_id"
LEFT JOIN "public"."alarm_configuration" "Alarm Configuration" ON "Zone"."zone_id" = "Alarm Configuration"."zone_id"
WHERE "Zone"."deleted" = FALSE
ORDER BY "Zone"."organization_id" DESC
LIMIT 1048575'''


zone_query = '''SELECT "public"."postal_address"."country" AS "country", "Zone"."zone_id" AS "Zone__zone_id"
FROM "public"."postal_address" INNER JOIN "public"."zone" "Zone" ON "public"."postal_address"."postal_address_id" = "Zone"."postal_address_id"
LEFT JOIN "public"."alarm_configuration" "Alarm Configuration" ON "Zone"."zone_id" = "Alarm Configuration"."zone_id"
WHERE "Zone"."deleted" = FALSE
ORDER BY "Zone"."organization_id" DESC
LIMIT 1048575'''


alarm_query = '''SELECT "public"."postal_address"."country" AS "country", "Alarm Configuration"."alarm_configuration_id" AS "Alarm Configuration__alarm_configuration_id"
FROM "public"."postal_address" INNER JOIN "public"."zone" "Zone" ON "public"."postal_address"."postal_address_id" = "Zone"."postal_address_id"
LEFT JOIN "public"."alarm_configuration" "Alarm Configuration" ON "Zone"."zone_id" = "Alarm Configuration"."zone_id"
WHERE "Zone"."deleted" = FALSE
ORDER BY "Zone"."organization_id" DESC
LIMIT 1048575'''


# Fetch a single row using fetchone() method.
'''
cursor.execute(master_query)


for row in cursor:
 print(row)


#Closing the connection
conn.close()
'''




#Counts the number of trigger devices in each zone
def count_triggers(zone_id, alarm_id):
   count_trigger_query = '''SELECT (
     (SELECT COUNT(*) FROM alarm_configuration_environmental_sensor WHERE alarm_configuration_id = '003B49EB-7C5C-4823-A1B6-913B2FDAA531') +
     (SELECT COUNT(*) FROM alarm_configuration_access_door WHERE alarm_configuration_id = '003B49EB-7C5C-4823-A1B6-913B2FDAA531') +
     (SELECT COUNT(*) FROM alarm_configuration_person_detected_camera WHERE alarm_configuration_id = '003B49EB-7C5C-4823-A1B6-913B2FDAA531') +
     (SELECT COUNT(*) FROM door_contact_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM glass_break_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM motion_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM panic_button WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM water_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM wired_door_contact_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM wired_glass_break_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM wired_motion_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM wired_panic_button WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted) +
     (SELECT COUNT(*) FROM wired_water_sensor WHERE zone_id = '7EFDFB5A-3B84-40BD-B179-6BE1804F35F2' AND NOT deleted)
   ) AS num_trigger_devices;'''


postal_id = 1
zone_id = 2
alarm_id = 3

with open('32.csv') as csv_file:
   for row in csv.reader(csv_file, delimiter=','):
      print(row[1:4])







