import re

class ConvertCoodinates:
    """Converts coordinates such as xxx to either a
    DMM or a DMS format"""

    def __init__(self):
        self.latitude = None
        self.longitude = None
    
    def __repr__(self):
        return f'{self.longitude} {self.latitude}'

    def __call__(self, latitude, longitude, as_dmm=True):
        if as_dmm:
            self.latitude = self.calculate_dmm(latitude)
            self.longitude = self.calculate_dmm(longitude)
        else:
            self.latitude = self.calculate_dms(latitude)
            self.longitude = self.calculate_dms(longitude)
        return f'{self.latitude} {self.longitude}'
    
    def parse_value(self, value):
        result = re.match(r'^(?P<degree>\-?\d+)\.(?P<remainder>\d+)$', str(value))
        if result:
            degree = result.groupdict()['degree']
            remainder = result.groupdict()['remainder']
            return degree, remainder
        return None, None

    def get_minutes(self, value):
        return round(float(value) * 60, 7)
    
    def calculate_dmm(self, value):
        degree, remainder = self.parse_value(value)
        minutes = self.get_minutes(f"0.{remainder}")
        return f'{degree}° {minutes}'

    def calculate_dms(self, value):
        degree, remainder = self.parse_value(value)
        minutes = self.get_minutes(f"0.{remainder}")

        result_seconds = re.search(r'\.(\d+)$', str(minutes))
        minutes_remainder = f'0.{result_seconds.group(1)}'
        seconds = round(float(minutes_remainder) * 60, 7)
        return f"""{degree}°{int(minutes)}'{int(seconds)}\""""
    

convert_coordinates = ConvertCoodinates()
