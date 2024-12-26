-- Q1
select date, avg((close-prev_close)/prev_close) as average_return
from (
    select date, ticker, close , lag(close) over(partition by ticker order by date) as prev_close
    from assignmen_saar.stocks_data
)
group by date;

-- Q2
select ticker, avg(close * volume) as avg_trade_worth
from assignmen_saar.stocks_data
group by ticker
order by avg_trade_worth desc
limit 1;

-- Q3
with daily_returns as(
select date, ticker, (close-prev_close)/prev_close as daily_return
from (select date, ticker, close , lag(close) over(partition by ticker order by date) as prev_close
from assignmen_saar.stocks_data)
)
select
ticker,
     -- this calculation was taken from the web, I didn't knew what annual stddev is, the assumption is there are 252 trading days per year
    sqrt(252) * stddev(daily_return) AS annualized_stddev
from daily_returns
group by ticker
order by annualized_stddev desc
limit 1;

-- Q4
with return_30_days as(
select date, ticker, (close-prev_30_close)/prev_30_close as return_30
from (select date, ticker, close , lag(close,30) over(partition by ticker order by date) as prev_30_close
from assignmen_saar.stocks_data)
)
select * from return_30_days
order by return_30 desc
limit 3;