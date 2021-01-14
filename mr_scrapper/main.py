from typing import AnyStr
from mr_scrapper.jobs.invest_get_ftse100.job_ftse100 import JobFtse100
from mr_scrapper.jobs.invest_get_sp500.job_sp500 import JobSp500
from mr_scrapper.jobs.invest_get_ftse250.job_ftse250 import JobFtse250
from mr_scrapper.job import Job


def get_job(job):
    if job == JobFtse100.__name__:
        return JobFtse100
    elif job == JobFtse250.__name__:
        return JobFtse250
    elif job == JobSp500.__name__:
        return JobSp500
    else:
        raise KeyError(f"what are you doing, you dont have a job called {job}... Think my boy")


def main(job: AnyStr):
    cls = get_job(job)
    job: Job = cls()
    job.run()


if __name__ == "__main__":
    main("JobFtse250")