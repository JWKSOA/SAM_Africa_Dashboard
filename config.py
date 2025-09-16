import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# SAM.gov API Configuration
SAM_API_KEY = os.getenv('SAM_API_KEY')
SAM_BASE_URL = 'https://api.sam.gov/opportunities/v2/search'

# African Countries (ISO 3-letter codes)
AFRICAN_COUNTRIES = [
    'DZA', 'AGO', 'BEN', 'BWA', 'BFA', 'BDI', 'CMR', 'CPV',
    'CAF', 'TCD', 'COM', 'COG', 'COD', 'CIV', 'DJI', 'EGY',
    'GNQ', 'ERI', 'ETH', 'GAB', 'GMB', 'GHA', 'GIN', 'GNB',
    'KEN', 'LSO', 'LBR', 'LBY', 'MDG', 'MWI', 'MLI', 'MRT',
    'MUS', 'MAR', 'MOZ', 'NAM', 'NER', 'NGA', 'RWA', 'STP',
    'SEN', 'SYC', 'SLE', 'SOM', 'ZAF', 'SSD', 'SDN', 'SWZ',
    'TZA', 'TGO', 'TUN', 'UGA', 'ZMB', 'ZWE'
]

# African Country Names Mapping
AFRICAN_COUNTRY_NAMES = {
    'DZA': 'Algeria', 'AGO': 'Angola', 'BEN': 'Benin', 'BWA': 'Botswana',
    'BFA': 'Burkina Faso', 'BDI': 'Burundi', 'CMR': 'Cameroon', 'CPV': 'Cape Verde',
    'CAF': 'Central African Republic', 'TCD': 'Chad', 'COM': 'Comoros', 'COG': 'Congo',
    'COD': 'Democratic Republic of Congo', 'CIV': 'Cote d\'Ivoire', 'DJI': 'Djibouti',
    'EGY': 'Egypt', 'GNQ': 'Equatorial Guinea', 'ERI': 'Eritrea', 'ETH': 'Ethiopia',
    'GAB': 'Gabon', 'GMB': 'Gambia', 'GHA': 'Ghana', 'GIN': 'Guinea',
    'GNB': 'Guinea-Bissau', 'KEN': 'Kenya', 'LSO': 'Lesotho', 'LBR': 'Liberia',
    'LBY': 'Libya', 'MDG': 'Madagascar', 'MWI': 'Malawi', 'MLI': 'Mali',
    'MRT': 'Mauritania', 'MUS': 'Mauritius', 'MAR': 'Morocco', 'MOZ': 'Mozambique',
    'NAM': 'Namibia', 'NER': 'Niger', 'NGA': 'Nigeria', 'RWA': 'Rwanda',
    'STP': 'Sao Tome and Principe', 'SEN': 'Senegal', 'SYC': 'Seychelles',
    'SLE': 'Sierra Leone', 'SOM': 'Somalia', 'ZAF': 'South Africa', 'SSD': 'South Sudan',
    'SDN': 'Sudan', 'SWZ': 'Eswatini', 'TZA': 'Tanzania', 'TGO': 'Togo',
    'TUN': 'Tunisia', 'UGA': 'Uganda', 'ZMB': 'Zambia', 'ZWE': 'Zimbabwe'
}

# Africa-related keywords for content filtering
AFRICA_KEYWORDS = [
    'africa', 'african', 'sub-saharan', 'horn of africa', 'west africa',
    'east africa', 'central africa', 'southern africa', 'north africa',
    'sahel', 'maghreb', 'african union', 'ecowas', 'sadc', 'eac'
]

# Enhanced Configuration for Historical Data Collection
DASHBOARD_TITLE = "SAM.gov Africa Opportunities Dashboard - Complete Historical Database"
UPDATE_INTERVAL_HOURS = int(os.getenv('UPDATE_INTERVAL_HOURS', 6))

# Historical data collection settings
HISTORICAL_YEARS_BACK = int(os.getenv('HISTORICAL_YEARS_BACK', 10))  # Default 10 years back
MAX_RESULTS_PER_REQUEST = 1000
SAM_GOV_OPPORTUNITY_BASE_URL = "https://sam.gov/opp/"