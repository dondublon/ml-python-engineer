class CompaniesManager:
    def __init__(self, db_server):
        self.data = db_server.get_companies_info()

    def get_companies(self, include_jointgs: bool):
        companies_only = self.data.companyName
        if include_jointgs:
            jointgs = [name for name in self.data.columns.to_list() if name.startswith('joint')]
            companies = list(companies_only) + jointgs
        else:
            companies = companies_only.to_list()
        return companies

    def get_jointg_companies(self, jointg_name):
        result = self.data.companyName[self.data[jointg_name]=='true']
        return result.to_list()