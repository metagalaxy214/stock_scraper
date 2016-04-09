require 'open-uri'
require 'nokogiri'
# require 'pry'
# require 'pry-nav'
# require 'axlsx'
require "csv"

class Advfn
  # ANNUAL_REPORT_URL = "http://www.advfn.com/stock-market/SIX/T/financials?btn=start_date&start_date=%%NUM%%&mode=annual_reports"  
  # ANNUAL_REPORT_URL = "http://www.advfn.com/stock-market/NASDAQ/%%COMPANY_SYMBOL%%/financials?btn=annual_reports&mode=company_data"
  ANNUAL_REPORT_URL = {
    'NASDAQ' => 'http://www.advfn.com/stock-market/NASDAQ/%%COMPANY_SYMBOL%%/financials?btn=start_date&start_date=%%NUM%%&mode=annual_reports',
    'NYSE' => 'http://www.advfn.com/stock-market/NYSE/%%COMPANY_SYMBOL%%/financials?btn=start_date&start_date=%%NUM%%&mode=annual_reports',
    'AMEX' => 'http://www.advfn.com/stock-market/AMEX/%%COMPANY_SYMBOL%%/financials?btn=start_date&start_date=%%NUM%%&mode=annual_reports',
  }  
  QUARTERLY_REPORT_URL = {
    'NASDAQ' => 'http://www.advfn.com/stock-market/NASDAQ/%%COMPANY_SYMBOL%%/financials?btn=istart_date&istart_date=%%NUM%%&mode=quarterly_reports',
    'NYSE' => 'http://www.advfn.com/stock-market/NYSE/%%COMPANY_SYMBOL%%/financials?btn=istart_date&istart_date=%%NUM%%&mode=quarterly_reports',
    'AMEX' => 'http://www.advfn.com/stock-market/AMEX/%%COMPANY_SYMBOL%%/financials?btn=istart_date&istart_date=%%NUM%%&mode=quarterly_reports',
  }
  COMPANY_URL = {
    'NASDAQ' => 'http://www.advfn.com/nasdaq/nasdaq.asp?companies=%%ALPHA%%',
    'NYSE' => 'http://www.advfn.com/nyse/newyorkstockexchange.asp?companies=%%ALPHA%%',
    'AMEX' => 'http://www.advfn.com/amex/americanstockexchange.asp?companies=%%ALPHA%%'
  }
  MARKET = ['NASDAQ', 'NYSE', 'AMEX']
  # MARKET = ['AMEX']
  ANNUAL_OUTPUT_FILE = 'output/annual/%%MARKET%%/annual_%%MARKET%%_%%ALPHA%%.csv'  
  QUARTERLY_OUTPUT_FILE = 'output/quarterly/%%MARKET%%/quarterly_%%MARKET%%_%%ALPHA%%.csv'  
  class << self

    def get_symbols market, alpha
      company_url = COMPANY_URL[market].gsub('%%ALPHA%%', alpha)
      company_doc = Nokogiri::HTML(open(company_url))
      company_doc.xpath('//table[contains(@class, "market tab1")]//tr[contains(@class, "ts")]').map{|t| t.xpath('.//td/a')[1].text.strip }
    end

    def get_symbols_from market, alpha, from_symbol
      company_url = COMPANY_URL[market].gsub('%%ALPHA%%', alpha)
      company_doc = Nokogiri::HTML(open(company_url))
      symbols = company_doc.xpath('//table[contains(@class, "market tab1")]//tr[contains(@class, "ts")]').map{|t| t.xpath('.//td/a')[1].text.strip }
      from_index = symbols.index(from_symbol)
      symbols[from_index..symbols.length]
    end

    def get_company_symbols market
      begin
        symbols = []
        ('A'..'Z').each do |alpha|
          company_url = COMPANY_URL[market].gsub('%%ALPHA%%', alpha)
          company_doc = Nokogiri::HTML(open(company_url))
          symbols = symbols | company_doc.xpath('//table[contains(@class, "market tab1")]//tr[contains(@class, "ts")]').map{|t| t.xpath('.//td/a')[1].text.strip }        
        end
        company_url = COMPANY_URL[market].gsub('%%ALPHA%%', '0')
        company_doc = Nokogiri::HTML(open(company_url))
        symbols = symbols | company_doc.xpath('//table[contains(@class, "market tab1")]//tr[contains(@class, "ts")]').map{|t| t.xpath('.//td/a')[1].text.strip }
      rescue StandardError => e
        puts "#{e}"        
      end
      symbols      
    end

    def scrape_market market
      threads = []
      ('A'..'Z').each do |alpha|
        threads << Thread.new { 
          scrape_market_with_alpha(market, alpha) 
        }        
      end
      threads.each(&:join)
    end
    def scrape_market_split market, alpha_from, alpha_to
      threads = []
      (alpha_from..alpha_to).each do |alpha|
        threads << Thread.new { 
          scrape_market_with_alpha(market, alpha) 
        }        
      end
      threads.each(&:join)
    end
    def scrape_market_quarterly market
      threads = []
      ('A'..'Z').each do |alpha|
        threads  << Thread.new { 
          scrape_market_with_alpha_quarterly(market, alpha) 
        }                   
      end
      threads.each(&:join)
    end
    def scrape_market_quarterly_split market, alpha_from, alpha_to
      threads = []
      (alpha_from..alpha_to).each do |alpha|
        threads  << Thread.new { 
          scrape_market_with_alpha_quarterly(market, alpha) 
        }                   
      end
      threads.each(&:join)
    end
    def scrape_market_with_alpha(market, alpha)      
      symbols = get_symbols(market, alpha)
      output_file_name = ANNUAL_OUTPUT_FILE.gsub('%%MARKET%%', market.downcase).gsub('%%ALPHA%%', alpha)
      write_header_columns output_file_name      
      symbols.each do |symbol|
        scrap_stock_detail market, symbol, output_file_name        
      end
      write_finish_mark output_file_name
    end
    def scrape_market_with_alpha_quarterly(market, alpha, from_symbol=nil)      
      symbols = get_symbols(market, alpha)      
      output_file_name = QUARTERLY_OUTPUT_FILE.gsub('%%MARKET%%', market.downcase).gsub('%%ALPHA%%', alpha)      
      write_header_columns_quarterly output_file_name
      symbols.each do |symbol|
        scrap_stock_detail_quarterly market, symbol, output_file_name                
      end
      write_finish_mark output_file_name
    end
    def scrap_stock_detail( market, company_symbol, output_file_name )

      ticker = company_symbol
      market_name = market.upcase
      headers = get_header_columns
      stock_url = ''      
      stock_url = ANNUAL_REPORT_URL[market].gsub('%%COMPANY_SYMBOL%%', company_symbol).gsub('%%NUM%%', '1')      
      stock_doc = Nokogiri::HTML(open(stock_url))      
      begin
        CSV.open(output_file_name, "a+b") do |csv|

          cur_index = 1
          last_index = stock_doc.xpath('//select[@id="start_dateid"]/option/@value').last.text.to_i
          loop do  

            year_end_date = stock_doc.xpath("//select[@id='start_dateid']/option[@value='#{cur_index}']").first.text          
            
            end_year_columns = stock_doc.xpath('//td[text()="year end date"]').first.parent.xpath('.//td').map(&:text)
            end_year_columns.delete('')
            cur_column_index = end_year_columns.index(year_end_date)          
            
            max_column_index = end_year_columns.count - 1
            (cur_column_index..max_column_index).each do |cur_column_num|              
              row = []
              year_end_date = end_year_columns[cur_column_num]
              cur_year = year_end_date.split('/').first
              headers.each do |head|          
                data_val = nil
                if head == 'url'
                  data_val = stock_url
                end
                if head == 'ticker'
                  data_val = company_symbol
                end
                if head == 'company name'            
                  data_val = stock_doc.xpath('//h1').text.strip
                end
                if head == 'market'
                  data_val = market_name
                end
                if head == 'year'
                  data_val = cur_year
                end
                if data_val.nil?            
                  head_td = stock_doc.xpath("//td[text()='#{head}']") 
                  value_tag = head_td.empty? ? nil : stock_doc.xpath("//td[text()='#{head}']").first.parent.xpath('.//td')[cur_column_num]
                  data_val = value_tag.nil? ? '' : value_tag.text
                end
                row << data_val            
              end
              
              csv << row
              cur_index = cur_index + 1
            end            
            break if cur_index > last_index
            stock_url = ANNUAL_REPORT_URL[market].gsub('%%COMPANY_SYMBOL%%', company_symbol).gsub('%%NUM%%', cur_index.to_s)
            stock_doc = Nokogiri::HTML(open(stock_url))
          end
        end
      rescue StandardError => e
        puts "#{e}"
        puts "#{market}, #{company_symbol}, #{stock_url}"        
      end
    end
    def scrap_stock_detail_quarterly( market, company_symbol, output_file_name )

      ticker = company_symbol
      market_name = market.upcase
      headers = get_header_columns_quarterly
      stock_url = ''      
      stock_url = QUARTERLY_REPORT_URL[market].gsub('%%COMPANY_SYMBOL%%', company_symbol).gsub('%%NUM%%', '1')           
      stock_doc = Nokogiri::HTML(open(stock_url))   
      begin
        CSV.open(output_file_name, "a+b") do |csv|
          cur_index = 1
          
          last_index = stock_doc.xpath('//select[@id="istart_dateid"]/option/@value').last.text.to_i
             
          loop do  
            year_end_date = stock_doc.xpath("//select[@id='istart_dateid']/option[@value='#{cur_index}']").first.text 
            end_year_columns = stock_doc.xpath('//td[text()="quarter end date"]').first.parent.xpath('.//td').map(&:text)
            end_year_columns.delete('')
            cur_column_index = end_year_columns.index(year_end_date)                      
            max_column_index = end_year_columns.count - 1            
            (cur_column_index..max_column_index).each do |cur_column_num|              
              
              row = []
              year_end_date = end_year_columns[cur_column_num]
              cur_year = year_end_date.split('/').first
              headers.each do |head|          
                data_val = nil
                if head == 'url'
                  data_val = stock_url
                end
                if head == 'ticker'
                  data_val = company_symbol
                end
                if head == 'company name'            
                  data_val = stock_doc.xpath('//h1').text.strip
                end
                if head == 'market'
                  data_val = market_name
                end
                if head == 'year'
                  data_val = cur_year
                end
                if data_val.nil?            
                  head_td = stock_doc.xpath("//td[text()='#{head}']") 
                  value_tag = head_td.empty? ? nil : stock_doc.xpath("//td[text()='#{head}']").first.parent.xpath('.//td')[cur_column_num]
                  data_val = value_tag.nil? ? '' : value_tag.text
                end
                row << data_val            
              end              
              csv << row
              cur_index = cur_index + 1
            end            
            break if cur_index > last_index
            stock_url = QUARTERLY_REPORT_URL[market].gsub('%%COMPANY_SYMBOL%%', company_symbol).gsub('%%NUM%%', cur_index.to_s)
            stock_doc = Nokogiri::HTML(open(stock_url))
          end
        end
      rescue StandardError => e
        puts "#{e}"
        puts "#{market}, #{company_symbol}, #{stock_url}"        
      end
    end

    def get_header_columns
      head_columns = []
      CSV.foreach("csv_template/annual_template.csv") do |row|
        head_columns = row
        break
      end
      head_columns
    end
    def get_header_columns_quarterly
      head_columns = []
      CSV.foreach("csv_template/quarterly_template.csv") do |row|
        head_columns = row
        break
      end
      head_columns
    end
    def write_header_columns output_file_name      
      headers = get_header_columns
      CSV.open(output_file_name, "w+b") do |csv|
        csv << headers
      end
    end
    def write_header_columns_quarterly output_file_name
      headers = get_header_columns_quarterly
      CSV.open(output_file_name, "w+b") do |csv|
        csv << headers
      end
    end
    def write_finish_mark output_file_name      
      CSV.open(output_file_name, "a+b") do |csv|
        csv << ["FINISHED"]
      end
    end

    def merge_files_annual market
      CSV.open("output/annual/annual_#{market.downcase}.csv", "w+") do |csv_sum|
        ('A'..'Z').each do |alpha|
          cur_index = 0
          CSV.foreach("output/annual/#{market.downcase}/annual_#{market.downcase}_#{alpha}.csv") do |row|                        
            if cur_index > 0 && row.first != 'FINISHED'
              csv_sum << row 
            end
            cur_index = cur_index + 1
          end
        end
        cur_index = 0
        CSV.foreach("output/annual/#{market.downcase}/annual_#{market.downcase}_0.csv") do |row|       
          if cur_index > 0 && row.first != 'FINISHED'
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
        cur_index = cur_index + 1
      end
    end

    def merge_files_quarterly market
      CSV.open("output/quarterly/quarterly_#{market.downcase}.csv", "w+") do |csv_sum|
        ('A'..'Z').each do |alpha|
          cur_index = 0
          CSV.foreach("output/quarterly/#{market.downcase}/quarterly_#{market.downcase}_#{alpha}.csv") do |row|            
            if cur_index > 0 && row.first != 'FINISHED'
              csv_sum << row 
            end
            cur_index = cur_index + 1
          end
        end
        cur_index = 0
        CSV.foreach("output/quarterly/#{market.downcase}/quarterly_#{market.downcase}_0.csv") do |row|       
          if cur_index > 0 && row.first != 'FINISHED'
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
      end
    end

    def merge_three_markets
      CSV.open("output/annual_report.csv", "w+") do |csv_sum|
        cur_index = 0
        headers = get_header_columns
        csv_sum << headers
        CSV.foreach("output/annual/annual_nasdaq.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
        cur_index = 0
        CSV.foreach("output/annual/annual_nyse.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
        cur_index = 0
        CSV.foreach("output/annual/annual_amex.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
      end
    end

    def merge_three_markets_quarterly
       CSV.open("output/quarterly_report.csv", "w+") do |csv_sum|
        headers = get_header_columns_quarterly
        csv_sum << headers
        cur_index = 0
        CSV.foreach("output/quarterly/quarterly_nasdaq.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
        cur_index = 0
        CSV.foreach("output/quarterly/quarterly_nyse.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
        cur_index = 0
        CSV.foreach("output/quarterly/quarterly_amex.csv") do |row|
          if cur_index > 0
            csv_sum << row 
          end
          cur_index = cur_index + 1
        end
      end
    end

  end
end