import pandera as pa
from datetime import datetime
from util.config import start_year, end_year


def get_wide_schema() -> pa.DataFrameSchema:

    schema = pa.DataFrameSchema(
        columns={
            # Basic identifiers
            "country_name": pa.Column(pa.String, nullable=False),  
            "country_iso3": pa.Column(
                pa.String,
                checks=[pa.Check.str_matches(r'^[A-Z]{3}$')], 
                nullable=False
            ),

            "year": pa.Column(
                pa.Int,
                checks=[
                    pa.Check.ge(start_year),       
                    pa.Check.le(end_year)
                ],
                nullable=False
            ),

            # Indicator columns
            "food_production_idx": pa.Column(pa.Float, nullable=True),

            "cereal_yield_kg_per_hectare": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      
                nullable=True
            ),

            "crop_production_idx": pa.Column(pa.Float, nullable=True),

            "agricultural_land_pct": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      
                nullable=True
            ),

            "gdp_per_capita_usd": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),     
                nullable=True
            ),

            "food_cpi_2010_base_100": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      
                nullable=True
            ),

            "food_imports_pct_merch": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),     
                nullable=True
            ),

            "food_exports_pct_merch": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),     
                nullable=True
            ),

            "population_total": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),     
                nullable=True
            ),

            "population_urban_pct": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      
                nullable=True
            ),

            "population_growth_annual_pct": pa.Column(
                pa.Float,
                nullable=True              
            ),
        },

        # DataFrame-level checks
        strict=True,                     
        unique=["country_iso3", "year"], # Ensure each country-year pair appears only once
    )

    return schema
