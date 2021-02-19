from typing import AnyStr
from mr_scrapper.jobs.invest_get_ftse.job_ftse import JobFtse
from mr_scrapper.jobs.invest_get_sp500.job_sp500 import JobSp500
from mr_scrapper.jobs.invest_get_nasdaq100.job_nasdaq100 import JobNasdaq100
from mr_scrapper.jobs.iexcloud_reference_data.job_iexcloud_reference_data import JobIEXCloudReferenceData
from mr_scrapper.jobs.iexcloud_company_financials.job_iexcloud_company_financials import JobIEXCloudCompanyFinancials
from mr_scrapper.jobs.yahoo_finance_financials.job_yahoofinance_company_financials import JobYahooFinanceCompanyFinancials
from mr_scrapper.jobs.zoopla_scrapper.job_zoopla import JobZoopla
from mr_scrapper.job import Job


def get_job(job):
    if job == JobFtse.__name__:
        return JobFtse
    elif job == JobSp500.__name__:
        return JobSp500
    elif job == JobNasdaq100.__name__:
        return JobNasdaq100
    elif job == JobIEXCloudReferenceData.__name__:
        return JobIEXCloudReferenceData
    elif job == JobIEXCloudCompanyFinancials.__name__:
        return JobIEXCloudCompanyFinancials
    elif job == JobYahooFinanceCompanyFinancials.__name__:
        return JobYahooFinanceCompanyFinancials
    elif job == JobZoopla.__name__:
        return JobZoopla
    else:
        raise KeyError(f"what are you doing, you dont have a job called {job}... Think my boy")


def main(job: AnyStr, **kwargs):
    cls = get_job(job)
    job: Job = cls()
    job.run(**kwargs)


if __name__ == "__main__":
    main("JobZoopla", replace=False)  # postcode="PE28",
