#! /bin/usr/python3

from iotclient import IotClient, load_template
import copy

def get_template_attribute(template, label_value):
	for attr in template['attrs']:
		if attr['label'] == label_value:
			return attr
	return None

iotc = IotClient()

template_lwm2m = iotc.create_template(load_template("template_lwm2m.json"))
template_dtls = iotc.create_template(load_template("template_dtls.json"))
template_temp_sensor = iotc.create_template(load_template("models/3303.json"))

device_endpoint = "client-endpoint"
device_payload_base = {
    "templates": [
				   template_lwm2m['id'],
                   template_dtls['id'],
                   template_temp_sensor['id']
                 ],
    "attrs": [
    	{
    		"label": "client_endpoint",
    		"static_value": device_endpoint,
    		"template_id": template_lwm2m['id'],
    		"id": get_template_attribute(template_lwm2m, "client_endpoint")['id']
    	}
    ],
    "label": "secure-dev"
}

for i in range(0, 3):
	print("---")
	device_payload = copy.deepcopy(device_payload_base)
	device_payload['label'] += '-' + str(i)
	device_payload['attrs'][0]['static_value'] += '-' + str(i)
	device_id = iotc.create_device(device_payload)
	print("device id: " + device_id)
	psk = iotc.gen_psk(device_id, 16)
	print("PSK: " + psk)

