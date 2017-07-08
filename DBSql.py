import pandas as pd
# import pymysql
from sqlalchemy import create_engine

e=create_engine("mysql+pymysql://root:123456@localhost/ppd_rjcorp?charset=utf8")
#
# users = pd.read_csv("~/Documents/T_project_user_info.csv",encoding="utf8", chunksize=100000)
# for user in users:
#     # print(user.columns)
#     user.columns=
#     # print(user.columns)
#     # print(user)
#     user.to_sql("user", e, if_exists="append")
#     # break
#
# project_approvals = pd.read_csv("~/Documents/T_project_approvalinfo.csv",encoding="utf8", chunksize=100000)
# for project_approval in project_approvals:
#     # print(user.columns)
#     project_approval.columns=['ID','ListingKey','BorrowerName','AuditProject','AuditStatus','ApprovalTime','CollectionTime']
#     # print(user.columns)
#     # print(user)
#     project_approval.to_sql("project_approval", e, if_exists="append")
#     # break
#
# project_mains = pd.read_csv("~/Documents/T_project_main.csv",encoding="utf8", chunksize=100000)
# for project_main in project_mains:
#     # print(user.columns)
#     project_main.columns=['ID','ListingKey','ProjectName','SecurityMark','CreditGrade','BorrowerName','Historical_SuccLoanTimes','AmountRequested','BorrowerRate','Term','RepaymentType','FundingProgress','BiddersNumber','RemainingTime','EndTime','Status','Balance','HukouVerification','DiplomaVerification','CreditVerification','Description','RayoffTimes','DelayTimesLessThan15D','DelayTimesGreaterThan15D','TotalAmountRequested','TotalAmountToBePaied','TotalAmountToBeCollected','BorrowerFirstSuccessLoanTime','RegisterTime','CollectionTime']
#     # print(user.columns)
#     # print(user)
#     project_main.to_sql("project_main", e, if_exists="append")
#     # break

def fetchToMysql(file,newvars, tableName):
    reader = pd.read_csv(file,encoding="utf8",low_memory=False, chunksize=100000, parse_dates=True)
    for r in reader:


        r.columns = newvars

        r.to_sql(tableName,e,if_exists="append")
        break

dir = "~/Documents/"


file = dir + "T_project_user_info.csv"
newvars=['MemberKey', 'ListingKey', 'UserName', \
         'LoanPurpose', 'Gender', 'Age', 'Marriage', \
         'Education', 'HouseOwnership', \
         'CarOwnership', 'CollectionTime']
tablename="users"
fetchToMysql(file,newvars,tablename)
print("table 3 success")
# file = dir + "T_userinfo_main.csv"
# newvars=['MemberKey', 'ListingKey', 'UserName', \
#          'LoanPurpose', 'Gender', 'Age', 'Marriage', \
#          'Education', 'HouseOwnership', \
#          'CarOwnership', 'CollectionTime']
# tablename= "users_main"
# fetchToMysql(file,newvars,tablename)
#
# print("table 5 success")


file= dir + "T_project_bid.csv"
newvars=['ID', 'ListingKey', 'BorrowerName',\
         'LoaningProjectName',\
         'Bidder',\
         'BidderURL',\
         'BidType',\
         'InterestRate',\
         'BiddingAmount',\
         'BiddingTime',\
         'CollectionTime'\
         ]
tablename="biddings"
fetchToMysql(file,newvars,tablename)

print("table 1 success")

file = dir+ "T_project_main.csv"
newvars=['ID','ListingKey','ProjectName','SecurityMark','CreditGrade',\
         'BorrowerName','Historical_SuccLoanTimes','Historical_FailedBidTimes',\
         'AmountRequested','BorrowerRate','Term','RepaymentType','FundingProgress',\
         'BiddersNumber','RemainingTime','EndTime','Status','Balance',\
         'HukouVerification','DiplomaVerification','CreditVerification',\
         'Description','RayoffTimes','DelayTimesLessThan15D','DelayTimesGreaterThan15D',\
         'TotalAmountRequested','TotalAmountToBePaied','TotalAmountToBeCollected',\
         'BorrowerFirstSuccessLoanTime','RegisterTime','CollectionTime']
# newvars=['ID','ListingKey','ProjectName','SecurityMark','CreditGrade','BorrowerName', \
#          'Historical_SuccLoanTimes','AmountRequested','BorrowerRate','Term', \
#          'RepaymentType','FundingProgress','BiddersNumber','RemainingTime','EndTime', \
#          'Status','Balance','HukouVerification','DiplomaVerification','CreditVerification', \
#          'Description','RayoffTimes','DelayTimesLessThan15D','DelayTimesGreaterThan15D', \
#          'TotalAmountRequested','TotalAmountToBePaied','TotalAmountToBeCollected', \
#          'BorrowerFirstSuccessLoanTime','RegisterTime','CollectionTime']

# ['id', 'pid', 'xmmc', 'aqbj', 'mjdj', 'jkyhm', 'cgcs', 'lbcs', 'jkze',
#        'jknll', 'jkqx', 'hkfs', 'jkjd', 'tbrs', 'sysj', 'jssj', 'xmzt', 'jkye',
#        'hkrz', 'xlrz', 'zxrz', 'jkxq', 'jkzchqcs', 'xyhqcs', 'dyhqcs',
#        'gjjrje', 'dhjrje', 'dstzje', 'dycjksj', 'zcsj', 'addtime']

tablename = "listings"
fetchToMysql(file,newvars,tablename)
print("table 2 success")






file = dir + "T_project_approvalinfo.csv"
newvars= ['ID','ListingKey','BorrowerName','AuditProject', \
          'AuditStatus','ApprovalTime','CollectionTime']
tablename= "project_approvals"
fetchToMysql(file,newvars,tablename)
print("table 4 success")

# file= dir + "T_userinfo_jklb.csv"
# newvars=
# tablename= "userinfo_jklb"
# fetchToMysql(file,newvars,tablename)



file= dir + "T_project_sixmonth.csv"
newvars=[ 'ID',  'ListingKey', 'BorrowerName',\
          'RepaymentDate','RepaymentTimes', \
          'RepaymentAmount','CollectionTime']
tablename="project_sixmonth"
fetchToMysql(file,newvars,tablename)
print("table 6 success")
