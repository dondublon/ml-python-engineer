class DBInterface:
    def get_companies_info(self):
        raise NotImplemented('must inherit')

    def get_data(self, company):
        raise NotImplemented('must inherit')