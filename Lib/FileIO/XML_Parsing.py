import json
import xmltodict

with open("/ES/ES_Bulk_Incre_Project/Lib/Feed_Config/Feed.xml", 'r') as f:
    xmlString = f.read()

print("xml input (xml_to_json.xml):")
print(xmlString)

jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)

print("\nJSON output(output.json):")
print(jsonString)

with open("/ES/ES_Bulk_Incre_Project/Lib/Feed_Config/xml_to_json.json", 'w') as f:
    f.write(jsonString)