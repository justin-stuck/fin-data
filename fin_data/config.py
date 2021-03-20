import logging
import os

from dotenv import load_dotenv

config_file = "../.env.paper"
ENV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), config_file
)
load_dotenv(ENV_PATH)


class Config:
    @staticmethod
    def get_value_from_env_variable(var_name):
        try:
            value = os.environ[var_name]
        except KeyError:
            logging.error(
                f"{var_name} env variable not set. "
                + "Please include in {config_file}."
            )
            raise
        return value

    @property
    def alpaca_url(self):
        return self.get_value_from_env_variable("ALPACA_URL")

    @property
    def alpaca_api_key(self):
        return self.get_value_from_env_variable("ALPACA_API_KEY")

    @property
    def alpaca_secret_key(self):
        return self.get_value_from_env_variable("ALPACA_SECRET_KEY")

    @property
    def db_path(self):
        db_name = self.get_value_from_env_variable("DB_NAME")
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)), f"database/{db_name}"
        )


config = Config()
