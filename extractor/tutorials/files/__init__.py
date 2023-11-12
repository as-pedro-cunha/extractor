import os
from extractor.utils import open_xml_as_txt


def get_nf1_xml():
    nf1_path = os.path.join(os.path.dirname(__file__), "nfe.xml")
    return open_xml_as_txt(nf1_path)
