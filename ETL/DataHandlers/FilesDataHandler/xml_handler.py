import xml.etree.ElementTree as ET

class XMLDataHandler():
    """Handler for XML data operations"""
    def __init__(self, filepath: str):
        self.filepath = filepath

    def read(self) -> ET.Element:
        """Read XML file and return root element"""
        try:
            tree = ET.parse(self.filepath)
            return tree.getroot()
        except FileNotFoundError:
            print(f"XML file not found: {self.filepath}")
            return None
        except ET.ParseError as e:
            print(f"Error parsing XML file: {e}")
            return None

    def write(self, data: ET.Element):
        """Write XML element to file"""
        try:
            tree = ET.ElementTree(data)
            tree.write(self.filepath, encoding='utf-8', xml_declaration=True)
        except IOError as e:
            print(f"Error writing XML file: {e}")
