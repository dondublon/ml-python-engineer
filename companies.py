class CompaniesManager:
    def __init__(self, db_server):
        self.data = db_server.get_companies_info()

    def get_companies(self, include_jointgs: bool):
        companies_only = self.data.companyName
        if include_jointgs:
            # TODO
            jointgs = [name for name in self.data.columns.to_list() if name.startswith('joint')]
            companies = list(companies_only) + jointgs
        else:
            companies = companies_only
        return companies