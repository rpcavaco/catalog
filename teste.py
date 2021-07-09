
import requests

THE_REQ_ORIG = """http://marselha.cm-porto.net/innergeovista/geoserver/PDM_revisao_2020out_DP/ows?service=WMS&version=1.3.0&sld_version=1.1.0&request=GetFeatureInfo&crs=EPSG%3A4326&bbox=41.112397138343539,-8.6757007047338028,41.20397757365015,-8.5627343001683887&format=image%2Fpng&width=721&height=774&layers=PDM_revisao_2020out_DP%3Apo1a_cqs_qsfuncional_pl&styles=&info_format=text/xml;%20subtype=gml/3.1.1&I=359&J=358&query_layers=PDM_revisao_2020out_DP%3Apo1a_cqs_qsfuncional_pl"""


THE_REQ = """http://marselha.cm-porto.net/innergeosrvr/PDM_revisao_2020out_DP/ows?service=WMS&version=1.3.0&sld_version=1.1.0&request=GetFeatureInfo&crs=EPSG%3A4326&bbox=41.112397138343539,-8.6757007047338028,41.20397757365015,-8.5627343001683887&format=image%2Fpng&width=721&height=774&layers=PDM_revisao_2020out_DP%3Apo1a_cqs_qsfuncional_pl&styles=&info_format=text/xml;%20subtype=gml/3.1.1&I=359&J=358&query_layers=PDM_revisao_2020out_DP%3Apo1a_cqs_qsfuncional_pl"""


def main():
	r = requests.get(THE_REQ)
	if r.status_code == 200:
		print(r.text)

if __name__ == "__main__":
	main()

