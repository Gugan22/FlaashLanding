package com.zetran.acct.bank;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.catalina.mapper.Mapper;
import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.opencsv.CSVReader;
import com.plaid.client.PlaidClient;
import com.plaid.client.request.AccountsBalanceGetRequest;
import com.plaid.client.request.ItemDeleteRequest;
import com.plaid.client.request.ItemPublicTokenExchangeRequest;
import com.plaid.client.request.TransactionsGetRequest;
import com.plaid.client.response.Account;
import com.plaid.client.response.AccountsBalanceGetResponse;
import com.plaid.client.response.ItemDeleteResponse;
import com.plaid.client.response.ItemPublicTokenExchangeResponse;
import com.plaid.client.response.TransactionsGetResponse;
import com.zetran.acct.buslayer.AccountServiceImpl;
import com.zetran.acct.buslayer.CommonTableServiceImpl;
import com.zetran.acct.buslayer.CurrencyServiceImpl;
import com.zetran.acct.buslayer.ErrorLogServiceImpl;
import com.zetran.acct.buslayer.EventServiceImpl;
import com.zetran.acct.buslayer.GLBalanceSheetReportServiceImpl;
import com.zetran.acct.buslayer.GeneralLedgerServiceImpl;
import com.zetran.acct.buslayer.ManualJournalServiceImpl;
import com.zetran.acct.buslayer.NewMassMigrationServiceImpl;
import com.zetran.acct.buslayer.OpeningBalanceServiceImpl;
import com.zetran.acct.buslayer.OrganizationServiceImpl;
import com.zetran.acct.buslayer.SubscriptionServiceImpl;
import com.zetran.acct.gso.NumberRangeUtil;
import com.zetran.acct.hibernate.dao.MybooksBalanceDaoImpl;
import com.zetran.acct.hibernate.entity.MybooksBalance;
import com.zetran.acct.pojo.BankAccount;
import com.zetran.acct.pojo.BankingException;
import com.zetran.acct.pojo.BusinessLayerException;
import com.zetran.acct.pojo.COAAccount;
import com.zetran.acct.pojo.Commit;
import com.zetran.acct.pojo.CommonTable;
import com.zetran.acct.pojo.Journal;
import com.zetran.acct.pojo.JournalItem;
import com.zetran.acct.pojo.ReadAttribute;
import com.zetran.acct.pojo.RejectRecords;
import com.zetran.acct.pojo.SubscriptionException;
import com.zetran.acct.pojo.UpdateAttribute;
import com.zetran.acct.pojo.UploadStatus;
import com.zetran.acct.pojo.ZetranGlobalException;
import com.zetran.acct.security.ApplicationContextUtil;
import com.zetran.acct.security.Encryption;
import com.zetran.acct.syc.BankingUtil;
import com.zetran.acct.syc.CommonGLReport;
import com.zetran.acct.syc.DashboardUtil;
//import com.zetran.acct.syc.CustomLogLevel;
import com.zetran.acct.syc.DateConversion;
import com.zetran.acct.syc.DateValidation;
import com.zetran.acct.syc.EntityUtil;
import com.zetran.acct.syc.FileAttachment;
import com.zetran.acct.syc.PlaidConnector;
import com.zetran.acct.syc.RandomNumberUtil;
import com.zetran.acct.syc.ReportFilters;
import com.zetran.acct.syc.SessionUtil;
import com.zetran.acct.syc.TransObj;
import com.zetran.acct.syc.TransactionUtil;

import retrofit2.Response;

/** Object ID : OBJ182 **/
@Service
@SuppressWarnings({"unchecked","unused"})
public class BankOverviewServiceImpl {

	@Autowired
	NumberRangeUtil util;

	@Autowired
	AccountServiceImpl accountService;

	@Autowired
	ReportFilters reportFilterService;

	@Autowired
	FileAttachment fileAttachmentService;

	@Autowired
	OrganizationServiceImpl organizationService;

	@Autowired
	CurrencyServiceImpl currencyService;

	@Autowired
	DateConversion dateConversion;

	@Autowired
	ErrorLogServiceImpl errorLogService;

	@Autowired
	PlaidConnector plaidConnecter;

	@Autowired
	@Value("${attchmnt_url_path}")
	private String attchmntUrlPath;

	@Autowired
	@Value("${bucket_name}")
	private String bucketName;

	@Autowired
	SessionUtil sessionUtil;

	@Autowired
	RandomNumberUtil randomNumberUtil;

	@Autowired
	BankTransactionsServiceImpl bankTransactionService;

	@Autowired
	EntityUtil entity;

	@Autowired
	EventServiceImpl eventService;

	@Autowired
	SubscriptionServiceImpl subscriptionService;

	@Autowired
	NewMassMigrationServiceImpl newMassMigrationService;

	@Autowired
	CommonGLReport commonGLReportService;

	@Autowired
	GLBalanceSheetReportServiceImpl GLBalanceSheetService;

	@Autowired
	BankingUtil bankingUtil;

	@Autowired
	OpeningBalanceServiceImpl openingBalanceService;

	@Autowired
	DashboardUtil dashboardUtil;

	@Autowired
	ManualJournalServiceImpl manualJournalService;

	@Autowired
	GeneralLedgerServiceImpl generalLedgerService;

	@Autowired
	DateValidation dateValidationService;

	@Autowired
	CommonTableServiceImpl commonTableService;

	@Autowired
	TransactionUtil transactionUtil;

	@Autowired
	MybooksBalanceDaoImpl mybooksBalanceDao;

	//Declaration of Logger for BankOverviewServiceImpl Class
	private static final Logger LOGGER = Logger.getLogger(BankOverviewServiceImpl.class);

	/**
	 * create bank account based on user information
	 * @param bankacc
	 * @return
	 * @throws Exception
	 */
	public  String addBankAccount(BankAccount bankacc) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try{
			if(bankacc.getDoc_dt() != null && !bankacc.getDoc_dt().isEmpty())
			{
				//convert to doc_dt for store
				String doc_dt = dateConversion.convertToStore(bankacc.getDoc_dt());
				// For Date Validation 
				dateValidationService.checkDateValidation(doc_dt);
			}

			bankacc.setOrg_id(org_id);
			bankacc.setBranch_id(branch_id);
			/** Checking the Account Code Validation **/
			accountCodeValidation(bankacc);
			/** Storing the Bank Information **/
			bankacc.setBank_acct_id(randomNumberUtil.getRandomNumber());
			/** Make default account as active **/
			bankacc.setActive_flg("T"); 

			/** Storing the Account Information in COA **/
			COAAccount account = new COAAccount();
			account.setAcct_id(bankacc.getBank_acct_id());
			account.setAcct_typ("Asset");
			account.setAcct_cat(bankacc.getAcct_cat());
			account.setAcct_code(bankacc.getAcct_code());
			account.setAcct_nm_sn(bankacc.getBank_acct_nm());
			account.setAcct_desc(bankacc.getBank_acct_desc());
			account.setBank_acct_num(bankacc.getBank_acct_num());
			account.setDeposit_acct_typ("Cash");
			account.setDoc_dt(bankacc.getDoc_dt());
			account.setAmount_amt(bankacc.getAmount_amt());
			account.setBranch_id(branch_id);
			String mulAccList=accountService.createAccount(account);
			JSONObject bankObj = new JSONObject();
			bankObj.put("bank_acct_id", bankacc.getBank_acct_id());
			//save the account opening balance in bank balance table
			refreshBankBalance(bankObj,org_id,user_id,branch_id);
			return mulAccList;
		}
		catch(ZetranGlobalException me)
		{ 
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		}
		catch(Exception ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}


	/**
	 * Retrieve the multiple Bank Accounts
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	public List<BankAccount> viewBankAccounts()
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			ReadAttribute readObj = new ReadAttribute();
			readObj.setOrg_id(org_id);
			readObj.setTableId("113");
			Map<String,String> bankFilter = new HashMap<>();
			Map<String,String> fieldMap = new HashMap<>();
			/*if(branch_id != null && !branch_id.isEmpty())
			{
				String filterExpression="";
				if(branch_id.equals("HeadQuarters"))
				{
					filterExpression = "((branch_id=:v_value1) or attribute_not_exists(branch_id))";
				}
				else
				{
					filterExpression = "(branch_id=:v_value1)";
				}
				readObj.setFilter(filterExpression);
				bankFilter.put("filterVal1",branch_id);
				fieldMap.put("branch_id",null);
			}*/
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			List<BankAccount> bankList = (List<BankAccount>)transObj.readTrans(readObj,bankFilter,fieldMap,"master","com.zetran.acct.pojo.BankAccount","","master",null);
			//get account information for get the account code
			Map<String, String> accMap=new HashMap<>();
			Map<String, String> accFilter=new HashMap<>();
			accMap.put("org_id", org_id);
			List<COAAccount> accList = accountService.readMultipleAccount(accMap, accFilter,fieldMap);
			Map<String,String> acctCodeMap = new HashMap<>();
			for(COAAccount acc : accList)
			{
				acctCodeMap.put(acc.getAcct_id(), acc.getAcct_code());
			}
			// For Getting Base Currency Symbol
			String curr_sym = currencyService.getBaseCurrencySymbol();
			//bankList = getMybooksBalance(bankList);
			//getMybooks balance 
			List<MybooksBalance> balanceList = mybooksBalanceDao.getMybooksBalanceList(org_id,branch_id);
			Map<String,Double> mybooksMap = new HashMap<>();
			Map<String,Double> bankMap = new HashMap<>();
			for(MybooksBalance mybooks : balanceList)
			{
				mybooksMap.put(mybooks.getBank_acct_id(), mybooks.getMybooks_balance());
				bankMap.put(mybooks.getBank_acct_id(), mybooks.getBank_balance());
			}
			List<BankAccount> list = new ArrayList<>();
			for (int bank = 0; bank < bankList.size(); bank++) {	
				bankList.get(bank).setAcct_code(acctCodeMap.get(bankList.get(bank).getBank_acct_id()));
				/**
				 * Putting the bank feed flag and removing the bank_security_key
				 **/
				if (bankList.get(bank).getBank_item_id() != null
						&& bankList.get(bank).getBank_security_key() != null && bankList.get(bank).getBank_item_id().equals("null") &&
						bankList.get(bank).getBank_security_key().equals("null")) {
					bankList.get(bank).setBank_security_key(null);
					bankList.get(bank).setBank_item_id(null);
					bankList.get(bank).setInstitute_name(null);
				}
				if (bankList.get(bank).getBank_item_id() != null
						&& bankList.get(bank).getBank_security_key() != null) {
					// putting bank_feed_flg as true
					bankList.get(bank).setBank_feeds_flg(true);
					// removing the 'bank_security_key', no need to pass
					// confidential info to client
					bankList.get(bank).setBank_security_key(null);
				}
				//for icici bank the security key not available so check the institute name
				if(bankList.get(bank).getInstitute_name() != null && bankList.get(bank).getInstitute_name().equals("ICICI"))
				{
					// putting bank_feed_flg as true
					bankList.get(bank).setBank_feeds_flg(true);
				}
				//convert to display the date
				if (bankList.get(bank).getDoc_dt() != null && !bankList.get(bank).getDoc_dt().isEmpty()) {
					bankList.get(bank).setDoc_dt(dateConversion.convertToDisplayinSession(bankList.get(bank).getDoc_dt()));	
				}
				bankList.get(bank).setCurr_sym(curr_sym);
				//set the latest balance info
				if(bankMap.get(bankList.get(bank).getBank_acct_id()) != null)
				{
					bankList.get(bank).setBank_bal_amt(Precision.round(bankMap.get(bankList.get(bank).getBank_acct_id()), 2));
				}
				if(mybooksMap.get(bankList.get(bank).getBank_acct_id()) != null)
				{
					bankList.get(bank).setMybooks_bal_amt(Precision.round(mybooksMap.get(bankList.get(bank).getBank_acct_id()), 2));
				}
				if(bankList.get(bank).getAcct_cat() == null || bankList.get(bank).getAcct_cat()!= null && bankList.get(bank).getAcct_cat().isEmpty())
				{
					bankList.get(bank).setAcct_cat("Cash");
				}
				if(bankList.get(bank).getActive_flg() != null && bankList.get(bank).getActive_flg().equals("T"))
				{
					list.add(bankList.get(bank));
				}	
			}

			LOGGER.info("Successfully Retrieved the multiple Bank Accounts !");
			return list;
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * updateBankAccount will update the bank account
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	public String updateBankAccount(BankAccount bankacc)
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			if(bankacc.getDoc_dt() != null && !bankacc.getDoc_dt().isEmpty())
			{
				//convert to doc_dt for store
				String doc_dt = dateConversion.convertToStore(bankacc.getDoc_dt());
				// For Date Validation 
				dateValidationService.checkDateValidation(doc_dt);
			}
			//get previous data for put bank security key information
			bankacc.setOrg_id(org_id);
			bankacc.setUser_id(user_id);
			bankacc.setBranch_id(branch_id);
			List<BankAccount> list = readBankAccount(bankacc);
			if(list != null && list.size() !=0)
			{
				BankAccount ba = list.get(0);
				bankacc.setBank_security_key(ba.getBank_security_key());
			}
			// Storing the Account Information in COA
			bankacc.setOrg_id(org_id);
			bankacc.setRange_key("range_key");
			//store in coa account
			COAAccount account = new COAAccount();
			account.setAcct_id(bankacc.getBank_acct_id());
			account.setAcct_typ("Asset");
			account.setAcct_cat(bankacc.getAcct_cat());
			account.setAcct_nm_sn(bankacc.getBank_acct_nm());
			account.setAcct_desc(bankacc.getBank_acct_desc());
			account.setAcct_code(bankacc.getAcct_code());
			account.setActive_flg(bankacc.getActive_flg());
			account.setBranch_id(branch_id);
			bankacc.setDeposit_acct_typ("Cash");
			Boolean trans_check = false;
			if(bankacc.getActive_flg() != null && bankacc.getActive_flg().equals("F"))
			{
				trans_check = true;
			}
			accountService.updateAccount(account,trans_check);

			//update opening balance details
			Thread t2 = null;
			Thread t1 = null;
			//delete the old opening balance journal data
			if(bankacc.getRef_doc_id() != null && !bankacc.getRef_doc_id().isEmpty())
			{
				Journal journ = new Journal();
				journ.setDoc_id(bankacc.getRef_doc_id());
				journ = manualJournalService.setGLHeaderLineItems(journ,null);
				if(journ != null)
				{
					ObjectMapper mapper = new ObjectMapper();
					Journal journal = new Journal();
					List<Map<String, String>> liMaps = new ArrayList<>();
					for(JournalItem jl : journ.getLineItem()) {
						Map<String, String> map = new HashMap<>();
						map = mapper.convertValue(jl, new TypeReference<Map<String, String>>() {});
						liMaps.add(map);
					}
					myBooksBalanceUpdate(liMaps, "update");
					t2 = generalLedgerService.<Journal>deleteGL(journ);
				}
			}
			//create new opening balance journal data
			ObjectMapper mapper = new ObjectMapper();
			Map<String,Object> bankObj = mapper.convertValue(bankacc,Map.class);
			if(bankacc.getDoc_dt() != null && !bankacc.getDoc_dt().isEmpty())
			{
				if(bankacc.getAmount_amt()!=0)
				{
					accountOpeningBalance(org_id,user_id,bankObj,branch_id);
				}
			}
			if(bankObj.get("ref_doc_id") != null && !bankObj.get("ref_doc_id").toString().isEmpty())
			{
				bankacc.setRef_doc_id(bankObj.get("ref_doc_id").toString());
			}
			if(bankObj.get("doc_dt") != null && !bankObj.get("doc_dt").toString().isEmpty())
			{
				bankacc.setDoc_dt(bankObj.get("doc_dt").toString());
			}
			/** For Transaction Management **/
			TransObj transObj = new TransObj();
			transObj.addItemsForInsert(bankacc, "113");
			Commit comObj = new Commit();
			comObj.setObj_id(bankacc.getBank_acct_id());
			comObj.setObj_typ("Bank"); 
			comObj.setBranch_id(branch_id);
			String resMsg = transObj.commitTransaction(comObj,bankacc,branch_id);
			JSONObject bankObj1 = new JSONObject();
			bankObj1.put("bank_acct_id", bankacc.getBank_acct_id());
			//save the account opening balance in bank balance table
			refreshBankBalance(bankObj1,org_id,user_id,branch_id);
			if(t2 != null)
			{
				t2.start();
			}
			if(t1 != null)
			{
				t1.start();
			}
			return resMsg;
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * readBankAccount will retrieve the Bank Account Information
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	public List<BankAccount> readBankAccount(BankAccount bankacc) throws Exception {
		String org_id=entity.getSessionAttributeValues(bankacc,"organizationId","org_id");
		String user_id=entity.getSessionAttributeValues(bankacc,"userId","user_id");
		String branch_id=entity.getSessionAttributeValues(bankacc,"branch_id","branch_id");
		try {
			bankacc.setOrg_id(org_id);
			Map<String,String> objFilterMap=new HashMap<String,String>();
			ReadAttribute readObj = new ReadAttribute();
			readObj.setOrg_id(bankacc.getOrg_id());
			readObj.setRange_key("range_key");
			readObj.setTableId("113");
			objFilterMap.put("filterAttr", "bank_acct_id");
			objFilterMap.put("filterVal", bankacc.getBank_acct_id());
			Map<String,String> fieldMap = new HashMap<>();
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			List<BankAccount> list = (List<BankAccount>)transObj.readMultipleTrans(readObj,objFilterMap,fieldMap,"master","com.zetran.acct.pojo.BankAccount",null);
			//getMybooks balance 
			List<MybooksBalance> balanceList = mybooksBalanceDao.getMybooksBalanceList(org_id,branch_id);
			Map<String,Double> mybooksMap = new HashMap<>();
			Map<String,Double> bankMap = new HashMap<>();
			for(MybooksBalance mybooks : balanceList)
			{
				mybooksMap.put(mybooks.getBank_acct_id(), mybooks.getMybooks_balance());
				bankMap.put(mybooks.getBank_acct_id(), mybooks.getBank_balance());
			}
			//set the latest balance information for this acct id
			for(int i=0;i<list.size();i++)
			{
				if(bankMap.get(list.get(i).getBank_acct_id()) != null)
				{
					list.get(i).setBank_bal_amt(Precision.round(bankMap.get(list.get(i).getBank_acct_id()), 2));
				}
				if(mybooksMap.get(list.get(i).getBank_acct_id()) != null)
				{
					list.get(i).setMybooks_bal_amt(Precision.round(mybooksMap.get(list.get(i).getBank_acct_id()), 2));
				}
			}
			return list;
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * sampleFile method return the default bank statement file
	 * 
	 * @throws Exception
	 */
	public S3Object sampleFile(String type) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String baseCountry = entity.getBaseCountry();
		try {
			if(baseCountry != null && baseCountry.equals("USA"))
			{
				if(type != null && type.equals("Single"))
				{
					S3Object object = fileAttachmentService.downloadFile(bucketName, "samplebanktransfile_single"
							+ "/" + "zetran_bankstatement.csv");
					return object;
				}
				else
				{
					S3Object object = fileAttachmentService.downloadFile(bucketName, "samplebanktransfile"
							+ "/" + "zetran_bankstatement.csv");
					return object;
				}
			}
			if(baseCountry != null && baseCountry.equals("IN"))
			{
				if(type != null && type.equals("Single"))
				{
					S3Object object = fileAttachmentService.downloadFile(bucketName, "samplebanktransfile_single"
							+ "/" + "zetran_ind_bankstatement.csv");
					return object;
				}
				else
				{
					S3Object object = fileAttachmentService.downloadFile(bucketName, "samplebanktransfile"
							+ "/" + "zetran_ind_bankstatement.csv");
					return object;
				}
			}
			return null;
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * will update the mybooks balance
	 * 
	 * @param bankArray
	 * @throws Exception
	 */
	public String updateMyBooksBalance(List<BankAccount> bankArray) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id=entity.getBranchId();
		try {
			/*List<UpdateAttribute> list = new ArrayList<>();
			for (int i = 0; i < bankArray.size(); i++) {
				BankAccount bank = bankArray.get(i);
				UpdateAttribute updateObj = new UpdateAttribute();
				updateObj.setBank_acct_id(bank.getBank_acct_id());
				updateObj.setOrg_id(org_id);
				updateObj.setColName1("mybooks_bal_amt");
				updateObj.setColValue1(Double.toString(bank.getMybooks_bal_amt()));
				updateObj.setRange_key("range_key");
				updateObj.setTableId("113");
				list.add(updateObj);
			}
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			if(list.size() != 0)
			{
				transObj.updateMultipleAttribute(list,"master"); 
			}
			return "Bank Balance Updated";*/
			List<MybooksBalance> balanceList = new ArrayList<>();
			for (int i = 0; i < bankArray.size(); i++) {
				MybooksBalance balance = new MybooksBalance();
				balance.setOrg_id(org_id);
				balance.setBank_acct_id(bankArray.get(i).getBank_acct_id());
				balance.setBranch_id(branch_id);
				balance.setMybooks_balance(bankArray.get(i).getMybooks_bal_amt());
				balanceList.add(balance);
			}
			if(balanceList.size() != 0)
			{
				mybooksBalanceDao.saveMybooksBalance(balanceList);
			}
			return "mybooks balance updated";
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}


	/**
	 * will update the balance for GL entry enabled objects
	 * 
	 * @param objItems
	 * @throws Exception
	 */
	public String myBooksBalanceUpdate(List<Map<String,String>> objItems,String methodType) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try {
			//get the latest balance information
			List<BankAccount> bankList = viewBankAccounts();

			for (int i = 0; i < bankList.size(); i++) {
				for (int j = 0; j < objItems.size(); j++) {
					if (bankList.get(i).getBank_acct_id()!= null
							&& objItems.get(j).containsKey("acct_id")) {
						if (bankList.get(i).getBank_acct_id().equals(objItems.get(j).get("acct_id"))) {
							double mybooks_bal_amt = bankList.get(i).getMybooks_bal_amt();
							if(methodType.equals("update") || methodType.equals("delete"))
							{
								double debit = 0.0;
								double credit = 0.0;
								
								if(objItems.get(j).get("debit")!=null)
								debit = Double.parseDouble(objItems.get(j).get("debit"));
								if(objItems.get(j).get("credit")!=null)
								credit = Double.parseDouble(objItems.get(j).get("credit"));
								if(credit!=0)
								{
									mybooks_bal_amt = credit;
								}
								if(debit!=0)
								{
									mybooks_bal_amt=debit;
								}
								bankList.get(i).setMybooks_bal_amt(mybooks_bal_amt);
							}
							else
							{
								double debit = Double.parseDouble(objItems.get(j).get("debit"));
								double credit = Double.parseDouble(objItems.get(j).get("credit"));
								// For Debit value
								if (debit != 0) {
									mybooks_bal_amt = mybooks_bal_amt + debit;
									bankList.get(i).setMybooks_bal_amt(mybooks_bal_amt);
								}
								if (credit != 0) {
									mybooks_bal_amt = mybooks_bal_amt - credit;
									bankList.get(i).setMybooks_bal_amt(mybooks_bal_amt);
								}
							}                                                                                                                                                
						}
					}
				}
			}
			//update the new balance 
			String resMsg = updateMyBooksBalance(bankList);
			if (resMsg != null) {
				return "Mybooks Balance Updated";
			}
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}


	/**
	 * addBankFeedAccount method will add bank feeds 
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	public String addBankFeedAccount(JSONObject bankMap) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			/** Getting the Plaid connection **/
			PlaidClient plaidClient = plaidConnecter.getPlaidConnection();

			/** Getting the access token and item_id through public token **/
			String mulAccList = null;
			Response<ItemPublicTokenExchangeResponse> response = plaidClient.service().itemPublicTokenExchange(new ItemPublicTokenExchangeRequest(bankMap.optString("public_token"))).execute();
			if(response.body()!=null)
			{
				String accessToken = response.body().getAccessToken();
				String itemId = response.body().getItemId();

				// Getting the List of Bank's Accounts
				Response<AccountsBalanceGetResponse> accountResponse = plaidClient.service().accountsBalanceGet(new AccountsBalanceGetRequest(accessToken)).execute();

				// Getting the each bank account from plaid account response
				for (Account Account : accountResponse.body().getAccounts()) {
					String  bank_acct_id=Account.getAccountId();
					Map<String, String> bankFeedMap = new HashMap<String, String>();
					bankFeedMap.put("org_id", org_id);
					bankFeedMap.put("bank_acct_id",bank_acct_id);
					bankFeedMap.put("active_flg", "T"); // Make default account as active
					bankFeedMap.put("bank_security_key", accessToken);
					bankFeedMap.put("bank_item_id", itemId);
					bankFeedMap.put("bank_bal_amt", Double.toString(Account.getBalances().getCurrent()));
					// Bank Feeds Time Stamp
					Timestamp creationDate = new Timestamp(System.currentTimeMillis());
					bankFeedMap.put("bankfeed_timestamp", creationDate.toString());
					// Storing the Account Information in COA
					if (Account.getSubtype().equals("credit card")) {
						bankFeedMap.put("acct_id",bank_acct_id);
						bankFeedMap.put("acct_typ", "Liability");
						bankFeedMap.put("acct_cat", "Accounts Payable");
						bankFeedMap.put("acct_nm_sn", Account.getName());
						// Passing the acct_typ and initial acc code to get the unduplicated acct_code
						bankFeedMap.put("acct_code", accountService.generateAccountCode("Liability", 2000));
						bankFeedMap.put("acct_desc", Account.getName());
						bankFeedMap.put("deposit_acct_typ", "Credit Card");
					} else {
						bankFeedMap.put("acct_id",bank_acct_id);
						bankFeedMap.put("acct_typ", "Asset");
						bankFeedMap.put("acct_cat", "Cash");
						bankFeedMap.put("acct_nm_sn", Account.getName());
						// Passing the acct_typ and initial acc code to get the unduplicated acct_code
						bankFeedMap.put("acct_code", accountService.generateAccountCode("Asset", 1000));
						bankFeedMap.put("acct_desc", Account.getName());
						bankFeedMap.put("deposit_acct_typ", "Cash");
					}
					String institute_name = bankMap.optJSONObject("metadata").optJSONObject("institution").optString("name");
					bankFeedMap.put("institute_name",institute_name);
					bankFeedMap.put("branch_id",branch_id);
					JSONObject bankObj1 = new JSONObject(bankFeedMap);
					ObjectMapper mapper = new ObjectMapper();
					COAAccount account = mapper.readValue(bankObj1.toString(),COAAccount.class);
					//create account in coa and bank
					mulAccList = accountService.createAccount(account);
					// Calling the refresh Bank Feeds
					JSONObject bankFeeds=new JSONObject();
					bankFeeds.put("bank_acct_id",bank_acct_id);

					//create event 
					BankAccount bankacc = new BankAccount();
					bankacc.setOrg_id(org_id);
					bankacc.setUser_id(user_id);
					bankacc.setBank_acct_id(bank_acct_id);
					bankacc.setBank_acct_nm(Account.getName());
					bankacc.setBranch_id(branch_id);
					eventService.bankReconcillationEvent(bankacc,branch_id);
					//for get the latest bank feeds and create bank transaction
					refreshBankFeedsByAccount(bankFeeds);
				}
			}
			if (mulAccList != null) {
				LOGGER.info("Bank Account is Successfully Created !");
				return "Bank Account Created !";
			} else {
				LOGGER.info("Bank Account is Not Created !");
			}
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			subscriptionService.updateBankFeedCount(org_id,"bankFeeds",branch_id);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			subscriptionService.updateBankFeedCount(org_id,"bankFeeds",branch_id);
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}


	/**
	 * refreshBankFeedsByAccount method will add bank feeds 
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	public String refreshBankFeedsByAccount(JSONObject bankMap)
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			// For Creation date and getting the organization date format
			Map<String,String> dateRange = reportFilterService.getDateRange("Today",null);
			Date date5 = new SimpleDateFormat("yyyy-MM-dd").parse(dateRange.get("from_date"));
			String formatString = sessionUtil.getSessionValue("dateFormat");
			if (formatString.isEmpty()) {
				// Default Client PlugIn format
				formatString = "yyyy-MM-dd";
			}
			String dateString2 = new SimpleDateFormat(formatString).format(date5);

			// Getting the access_token and item_id through bank_acct_id
			BankAccount bankacc = new BankAccount();
			bankacc.setBank_acct_id(bankMap.optString("bank_acct_id"));
			bankacc.setBranch_id(branch_id);
			List<BankAccount> bankDetails = readBankAccount(bankacc);
			if(bankDetails.size() !=0)
			{
				BankAccount bankObj = bankDetails.get(0);
				if(bankObj.getActive_flg() != null && bankObj.getActive_flg().equals("F"))
				{
					LOGGER.info("Bankfeeds are disabled");
					throw new BankingException("Bankfeeds are disabled", 557,org_id,user_id);
				}

				String access_token = bankObj.getBank_security_key();
				// Getting the Plaid connection
				PlaidClient plaidClient = plaidConnecter.getPlaidConnection();
				if(access_token != null)
				{
					// Getting All accounts of user's Bank account
					Response<AccountsBalanceGetResponse> accountResponse = plaidClient.service().accountsBalanceGet(new AccountsBalanceGetRequest(access_token)).execute();
					if(accountResponse != null)
					{
						if(accountResponse.body() != null) {
							//get list of account details from plaid
							List<Account> bankAccountsList = accountResponse.body().getAccounts();
							ObjectMapper mapper = new ObjectMapper();
							JSONArray obj = new JSONArray(bankAccountsList);
							List<Map<String,String>> bankAccountsArray = mapper.readValue(obj.toString(),TypeFactory.defaultInstance().constructCollectionType(List.class,  
									Map.class));
							//System.out.println(bankAccountsArray);
							for (int i = 0; i < bankAccountsArray.size(); i++) {
								// Getting the transaction amount type
								if (bankMap.get("bank_acct_id").equals(bankAccountsArray.get(i).get("accountId"))) {
									bankMap.put("subtype", bankAccountsArray.get(i).get("subtype"));
								}
							}
						}
					}
				}

				// Getting Transactions of Bank account for last 90 days
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

				Date today = new Date();
				Calendar cal = new GregorianCalendar();
				cal.setTime(today);
				cal.add(Calendar.DAY_OF_MONTH, -90);
				String dateString4 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
				String dateString3 = new SimpleDateFormat("yyyy-MM-dd").format(today);

				Date startDate = simpleDateFormat.parse(dateString4);
				Date endDate = simpleDateFormat.parse(dateString3);

				// Getting the uncategorized existing transactions
				Map<String,String> bankTrnsMap=new HashMap<String,String>();
				bankTrnsMap.put("bank_acct_id",bankMap.getString("bank_acct_id"));
				List<Map<String,String>> unCatExistTransArray =bankTransactionService.readUncategorizedTransactions(bankTrnsMap);

				// Getting the categorized existing transactions
				Map<String,String> catBankTrnsMap=new HashMap<String,String>();
				catBankTrnsMap.put("bank_acct_id",bankMap.getString("bank_acct_id"));
				List<Map<String,String>> catExistTransArray=bankTransactionService.readCategorizedTransactions(catBankTrnsMap);
				
				//get transaction from plaid
				List<Map<String,Object>> bankTransArray = new ArrayList<>();
				if(access_token != null)
				{
					ObjectMapper mapper = new ObjectMapper();
					Response<TransactionsGetResponse> response = plaidClient.service().transactionsGet(new TransactionsGetRequest(access_token, startDate, endDate)).execute();
					if(response != null)
					{
						if(response.body() != null)
						{
							List<com.plaid.client.response.TransactionsGetResponse.Transaction> bankTransactionList = response.body().getTransactions();
							JSONArray obj = new JSONArray(bankTransactionList);
							bankTransArray = mapper.readValue(obj.toString(),TypeFactory.defaultInstance().constructCollectionType(List.class,  
									Map.class));
							//System.out.println("bankTransArray:"+bankTransArray);
						}
					}
				}

				DecimalFormat df = new DecimalFormat("####0.00");
				List<Map<String, Object>> bankTransList = new ArrayList<Map<String, Object>>();
				for (int j = 0; j < bankTransArray.size(); j++) {
					// If bank account's accountId is equal to transaction's accountId, then making the entry in bank_trans table
					if (bankMap.get("bank_acct_id").equals(bankTransArray.get(j).get("accountId"))) {
						Map<String, Object> bankTransMap = new HashMap<String, Object>();
						//check the bank trans id used in categorized transaction or not. if it is used then no need to create this
						JSONObject trnsObj=checkTrnsId(unCatExistTransArray,catExistTransArray,bankTransArray.get(j).get("transactionId").toString());
						String bank_trans_id=null;
						//this if check the categorize 
						if((!trnsObj.getString("bankTransactionId").equals("null") && (!trnsObj.getBoolean("trnsFlg"))))
						{
							continue;
						}
						//these 2 check the uncategorize
						if((!trnsObj.getString("bankTransactionId").equals("null") && trnsObj.getBoolean("trnsFlg")))
						{
							bank_trans_id=trnsObj.getString("bankTransactionId");
						}
						if(trnsObj.getString("bankTransactionId").equals("null") && (!trnsObj.getBoolean("trnsFlg")))
						{
							bank_trans_id=bankTransArray.get(j).get("transactionId").toString();
						}
						//create bank trans table entry
						bankTransMap.put("bank_trans_id",bank_trans_id);
						bankTransMap.put("trans_dt", dateConversion.convertToStoreForBankFeeds(bankTransArray.get(j).get("date").toString()));
						bankTransMap.put("trans_txt", bankTransArray.get(j).get("name"));
						ObjectMapper mapper = new ObjectMapper();
						String json = mapper.writeValueAsString(bankTransArray.get(j).get("paymentMeta"));
						JSONObject pymtObj = new JSONObject(json);
						if (!(pymtObj.isNull("referenceNumber"))) {
							bankTransMap.put("reference_sn", pymtObj.optString("referenceNumber"));
						}
						bankTransMap.put("creation_dt", dateConversion.convertToStore(dateString2));
						double amount_check=Double.parseDouble(bankTransArray.get(j).get("amount").toString());
						// For categorize the Debit and Credit amount
						/** Negative values are goes to CREDIT and 
						 * Positive values are going to DEBIT according to Plaid Documentation **/
						if(bankMap.has("subtype") && bankMap.getString("subtype").equals("Credit Card"))
						{
							if (amount_check<0) {
								bankTransMap.put("deb_cre_ind", "DR");
								bankTransMap.put("amount_amt",df.format(-Double.parseDouble((bankTransArray.get(j).get("amount").toString()))));
							} else {
								bankTransMap.put("deb_cre_ind", "CR");
								bankTransMap.put("amount_amt",df.format(Math.abs(Double.parseDouble(bankTransArray.get(j).get("amount").toString()))));
							}
						}
						else
						{
							if (amount_check<0) {
								bankTransMap.put("deb_cre_ind", "CR");
								bankTransMap.put("amount_amt",df.format(Math.abs(Double.parseDouble(bankTransArray.get(j).get("amount").toString()))));
							} else {
								bankTransMap.put("deb_cre_ind", "DR");
								bankTransMap.put("amount_amt",df.format(Double.parseDouble(bankTransArray.get(j).get("amount").toString())));
							}
						}
						bankTransMap.put("doc_id", bankTransMap.get("bank_trans_id"));
						bankTransMap.put("tableId", "114");
						bankTransMap.put("range_key", "doc_id");
						bankTransMap.put("org_id", org_id);
						bankTransMap.put("bank_acct_id", bankMap.optString("bank_acct_id"));
						bankTransMap.put("branch_id", branch_id);
						bankTransList.add(bankTransMap);
					}
				}

				if(bankTransList.size()!=0)
				{
					TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
					transObj.createOrUpdate(bankTransList, "transaction");
				}
				/*	else
				{
					LOGGER.info("Doesn't have any transactions for this account");
					throw new BankingException("Doesn't have any transactions for this account", 554,org_id,user_id);
				}*/
				LOGGER.info("Transactions are Successfully Refreshed !");
			}
		}catch(BankingException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch(ZetranGlobalException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}


	/**
	 * checkTrnsId will check and return the trnasactionId
	 * 
	 * @param existBankTransArray
	 *  * @param bankTrnsId
	 * @throws Exception
	 */
	public JSONObject checkTrnsId(List<Map<String,String>> unCatTransArray,List<Map<String,String>> catTransArray,String bankTrnsId)throws Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String bankTransactionId=null;
		JSONObject trnsObj=new JSONObject();
		trnsObj.put("bankTransactionId","null");
		trnsObj.put("trnsFlg",false);
		try
		{
			// Checking Uncategorized Transactions
			for(int i=0;i<unCatTransArray.size();i++)
			{
				if(unCatTransArray.get(i).get("bank_trans_id").equals(bankTrnsId))
				{
					bankTransactionId=unCatTransArray.get(i).get("bank_trans_id");
					trnsObj.put("bankTransactionId",bankTransactionId);
					trnsObj.put("trnsFlg",true);
				}
			}
			// Checking Categorized Transactions
			for(int i=0;i<catTransArray.size();i++)
			{
				if(catTransArray.get(i).get("bank_trans_id").equals(bankTrnsId))
				{
					bankTransactionId=catTransArray.get(i).get("bank_trans_id");
					trnsObj.put("bankTransactionId",bankTransactionId);
					trnsObj.put("trnsFlg",false);
				}
			}
			return trnsObj;
		}
		catch(Exception ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();
		}
		return trnsObj;
	}

	/**
	 * accountCodeValidation will validate account codes
	 * 
	 * @param bankMap
	 * @throws Exception
	 */
	private void accountCodeValidation(BankAccount bankacc)
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			/**
			 * Account Code Validation Must be with in Range of 1000-1999 and
			 * Should not be use existing codes
			 **/
			if(bankacc.getAcct_code() != null && !bankacc.getAcct_code().isEmpty())
			{
				int acct_code = Integer.parseInt(bankacc.getAcct_code());
				if (!((1000 >= acct_code) || (acct_code <= 1999))) {
					LOGGER.info("Account code entered should be between 1000 - 1999");
					throw new BusinessLayerException("Account code entered should be between 1000 - 1999", 738,org_id,user_id);
				}
				/** Checking the code is already exist or not **/
				Map<String, String> accMap=new HashMap<>();
				Map<String, String> accFilter=new HashMap<>();
				Map<String, String> fieldMap = new HashMap<>();
				accMap.put("org_id", bankacc.getOrg_id());
				String filterExpression = ("(acct_code=:v_value1)");
				accMap.put("filterExpression", filterExpression);
				accFilter.put("filterVal1", bankacc.getAcct_code());
				fieldMap.put("acct_code",null);
				/*if(branch_id != null && !branch_id.isEmpty())
				{
					if(branch_id.equals("HeadQuarters"))
					{
						filterExpression = filterExpression +" and ((branch_id=:v_value2) or attribute_not_exists(branch_id))";
					}
					else
					{
						filterExpression = filterExpression +" and (branch_id=:v_value2)";
					}
					accMap.put("filterExpression",filterExpression);
					accFilter.put("filterVal2",branch_id);
					fieldMap.put("branch_id",null);
				}*/
				List<COAAccount> accList = accountService.readMultipleAccount(accMap, accFilter,fieldMap);
				if(accList != null && accList.size() !=0)
				{
					LOGGER.info("Duplicate account code, please check");
					throw new BusinessLayerException("Duplicate account code, please check", 867,org_id,user_id);
				}
			}
		} catch (BusinessLayerException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		}
		catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}
	/**
	 * update the bank balance in bank account table
	 * @param bankArray
	 * @return
	 * @throws Exception
	 */
	public String updateBankBalance(List<BankAccount> bankArray) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			/*List<UpdateAttribute> list = new ArrayList<>();
			for (int i = 0; i < bankArray.size(); i++) {
				BankAccount bank = bankArray.get(i);
				UpdateAttribute updateObj = new UpdateAttribute();
				updateObj.setBank_acct_id(bank.getBank_acct_id());
				updateObj.setOrg_id(org_id);
				updateObj.setColName1("bank_bal_amt");
				updateObj.setColValue1(Double.toString(bank.getBank_bal_amt()));
				updateObj.setRange_key("range_key");
				updateObj.setTableId("113");
				list.add(updateObj);
			}
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			if(list.size() != 0)
			{
				transObj.updateMultipleAttribute(list,"master"); 
			}*/
			List<MybooksBalance> balanceList = new ArrayList<>();
			for (int i = 0; i < bankArray.size(); i++) {
				MybooksBalance balance = new MybooksBalance();
				balance.setOrg_id(org_id);
				balance.setBank_acct_id(bankArray.get(i).getBank_acct_id());
				balance.setBranch_id(branch_id);
				balance.setBank_balance(bankArray.get(i).getBank_bal_amt());
				balanceList.add(balance);
			}
			if(balanceList.size() != 0)
			{
				mybooksBalanceDao.saveMybooksBankBalance(balanceList);
			}
			return "Bank Balance Updated";
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}
	/** 
	 * update the bank balance for opening balance transaction
	 * @param objItems
	 * @param methodType
	 * @return
	 * @throws Exception
	 */
	public String bankBalanceUpdate(List<Map<String, String>> objItems,String methodType)
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try {
			// For Updating the Zetran Balance in Bank master table
			//get the latest balance details from this service
			List<BankAccount> bankList = viewBankAccounts();

			for (int i = 0; i < bankList.size(); i++) {
				for (int j = 0; j < objItems.size(); j++) {
					if (bankList.get(i).getBank_acct_id()!= null
							&& objItems.get(j).containsKey("acct_id")) {
						if (bankList.get(i).getBank_acct_id().equals(objItems.get(j).get("acct_id"))) {
							if(bankList.get(i).getBank_security_key() == null || (bankList.get(i).getBank_security_key() != null && bankList.get(i).getBank_security_key().isEmpty()))
							{
								double bank_bal_amt = bankList.get(i).getBank_bal_amt();
								double debit = Double.parseDouble(objItems.get(j).get("debit"));
								double credit = Double.parseDouble(objItems.get(j).get("credit"));
								if(methodType.equals("update") || methodType.equals("delete"))
								{
									// For Debit value
									if (debit != 0) {
										bank_bal_amt = bank_bal_amt - debit;
									}
									if (credit != 0) {
										bank_bal_amt = bank_bal_amt + credit;
									}
									bankList.get(i).setBank_bal_amt(bank_bal_amt);
								}
								else
								{
									// For Debit value
									if (debit != 0) {
										bank_bal_amt = bank_bal_amt + debit;
									}
									if (credit != 0) {
										bank_bal_amt = bank_bal_amt - credit;
									}
									bankList.get(i).setBank_bal_amt(bank_bal_amt);
								}
							}
						}
					}
				}
			}

			String resMsg = updateBankBalance(bankList);
			if (resMsg != null) {
				return "Bank Balance Updated";
			}
		}catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		}  
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}
	/**
	 * validate the upload bank file and send the reject information
	 * @param bankMap
	 * @param bankFlatFile
	 * @return
	 * @throws ZetranGlobalException
	 * @throws Exception
	 */
	public String validateBankFlatFile(Map<String, String> bankMap,MultipartFile bankFlatFile, String type)throws ZetranGlobalException,Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try
		{
			Map<String,String> map = new HashMap<>();
			List<RejectRecords> rejectList = new ArrayList<>();
			//validate the flat file
			map = flatFileValidation(bankMap,bankFlatFile,rejectList,type);
			ObjectMapper mapper = new ObjectMapper();
			String str = mapper.writeValueAsString(rejectList);
			JSONArray array = new JSONArray(str);

			JSONObject json = new JSONObject();
			json.put("total_count", map.get("total_count"));
			json.put("reject_count", map.get("reject_count"));
			json.put("rejectedList", array);
			return json.toString();
		}
		catch (SubscriptionException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} 
		catch (ZetranGlobalException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}
	/**
	 *  get session values for upload bank statement
	 * @return
	 * @throws Exception
	 */
	public Map<String,String> getSessionValues()throws Exception
	{
		Map<String,String> sessionMap = new HashMap<>();
		sessionMap.put("org_id", sessionUtil.getSessionValue("organizationId"));
		sessionMap.put("user_id", sessionUtil.getSessionValue("userId"));
		sessionMap.put("dateFormat", sessionUtil.getSessionValue("dateFormat"));
		sessionMap.put("fiscalYear", sessionUtil.getSessionValue("fiscalYear"));
		sessionMap.put("cashAccrualType", sessionUtil.getSessionValue("cashAccrualType"));
		sessionMap.put("baseCountry", sessionUtil.getSessionValue("baseCountry"));
		sessionMap.put("baseCurrency", currencyService.getBaseCurrencyCode());
		sessionMap.put("curr_sym", currencyService.getBaseCurrencySymbol());
		sessionMap.put("currencyId", sessionUtil.getSessionValue("currencyId"));
		sessionMap.put("in_GST_No", sessionUtil.getSessionValue("in_GST_No"));
		sessionMap.put("state_txt", sessionUtil.getSessionValue("state_txt"));
		sessionMap.put("branch_id", sessionUtil.getSessionValue("branch_id"));
		return sessionMap;
	}

	/** 
	 * simulate the upload bank statement file , this function not create the transaction , just check the file if the transaction able to create or not
	 * @param bankMap
	 * @param bankFlatFile
	 * @return
	 * @throws ZetranGlobalException
	 * @throws Exception
	 */
	public String simulateBankFlatFile(Map<String, String> bankMap,MultipartFile bankFlatFile, String type) throws ZetranGlobalException,Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try
		{
			Thread t1 = null;
			String status_doc_id = randomNumberUtil.getRandomNumber();
			Map<String,String> map = new HashMap<>();
			Map<String,String> sessionInfo = getSessionValues();
			List<RejectRecords> rejectList = new ArrayList<>();
			//validate the flat file
			Map<String,String> flat_file_map = flatFileValidation(bankMap,bankFlatFile,rejectList,type);
			map.putAll(flat_file_map);
			final String doc_id = status_doc_id;
			t1 = new Thread() {
				public void run()
				{
					try {
						createFlatFileTransaction(bankMap,bankFlatFile,"simulate",rejectList,sessionInfo,flat_file_map,type,doc_id);				
					} catch (ZetranGlobalException e) {
						String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+e ;
						LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
					} catch (Exception e) {
						String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+e ;
						LOGGER.log(Level.ERROR, message);
					}
				}
			};

			ObjectMapper mapper = new ObjectMapper();
			String str = mapper.writeValueAsString(rejectList);
			JSONArray array = new JSONArray(str);
			JSONObject json = new JSONObject();
			json.put("total_count", map.get("total_count"));
			json.put("reject_count", map.get("reject_count"));
			json.put("rejectedList", array);
			int accept_count = Integer.parseInt(json.getString("total_count")) - Integer.parseInt(json.getString("reject_count"));
			if(accept_count >0)
			{
				//create the upload status for simulate
				Map<String,Integer> countMap = new HashMap<>();
				countMap.put("record_count", Integer.parseInt(json.getString("total_count")));
				countMap.put("create_count", accept_count);
				UploadStatus uploadStatus = newMassMigrationService.uploadStatus(org_id,user_id,"Bank",rejectList,countMap,"simulate",branch_id,doc_id,true);
				status_doc_id = uploadStatus.getDoc_id();
				t1.start();
			}
			return json.toString();

		}
		catch (SubscriptionException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} 
		catch (ZetranGlobalException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}

	}

	/** 
	 * save the upload bank statement file , this function create the transaction based on the statement file
	 * @param bankMap
	 * @param bankFlatFile
	 * @return
	 * @throws ZetranGlobalException
	 * @throws Exception
	 */
	public String saveBankFlatFile(Map<String, String> bankMap,MultipartFile bankFlatFile,String type) throws ZetranGlobalException,Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try
		{
			Thread t1 = null;
			String status_doc_id = randomNumberUtil.getRandomNumber();
			Map<String,String> map = new HashMap<>();
			Map<String,String> sessionInfo = getSessionValues();
			List<RejectRecords> rejectList = new ArrayList<>();
			//validate the flat file
			Map<String,String> flat_file_map = flatFileValidation(bankMap,bankFlatFile,rejectList,type);
			map.putAll(flat_file_map);
			final String doc_id = status_doc_id;
			t1 = new Thread() {
				public void run()
				{
					try {
						//create flat file based bank transaction
						createFlatFileTransaction(bankMap,bankFlatFile,"save",rejectList,sessionInfo,flat_file_map,type,doc_id);
						JSONObject bankObj = new JSONObject();
						bankObj.put("bank_acct_id",bankMap.get("bank_acct_id"));
						//after create call the bank balance update service
						refreshBankBalance(bankObj,org_id,user_id,branch_id);
						bankingUtil.bankBalanceCalculation(bankMap.get("bank_acct_id"), null, org_id, user_id,branch_id);
					} catch (ZetranGlobalException e) {
						String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+e ;
						LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
					} catch (Exception e) {
						String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+e ;
						LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
					}
				}
			};

			ObjectMapper mapper = new ObjectMapper();
			String str = mapper.writeValueAsString(rejectList);
			JSONArray array = new JSONArray(str);
			JSONObject json = new JSONObject();
			json.put("total_count", map.get("total_count"));
			json.put("reject_count", map.get("reject_count"));
			json.put("rejectedList", array);
			int accept_count = Integer.parseInt(json.getString("total_count")) - Integer.parseInt(json.getString("reject_count"));
			if(accept_count >0)
			{
				//create upload status
				Map<String,Integer> countMap = new HashMap<>();
				countMap.put("record_count", Integer.parseInt(json.getString("total_count")));
				countMap.put("create_count", accept_count);
				UploadStatus uploadStatus = newMassMigrationService.uploadStatus(org_id,user_id,"Bank",rejectList,countMap,"save",branch_id,doc_id,true);
				t1.start();
			}
			return json.toString();
		}
		catch (SubscriptionException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} 
		catch (ZetranGlobalException me) {
			me.printStackTrace();
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}

	}

	/**
	 * Will upload the Bank Flat file to Bank Accounts
	 * 
	 * @param bankMap
	 * @param bankFlatFile
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public void createFlatFileTransaction(Map<String, String> bankMap,MultipartFile bankFlatFile,String mode,List<RejectRecords> rejectList,
			Map<String,String> sessionInfo,Map<String,String> flat_file_map,String type,String doc_id) throws Exception {
		String org_id=sessionInfo.get("org_id");
		String user_id=sessionInfo.get("user_id");
		String baseCountry = sessionInfo.get("baseCountry");
		String branch_id = sessionInfo.get("branch_id");
		if(bankFlatFile == null)
		{
			LOGGER.info("Bank statements is in incorrect file format, Please use csv(comma delimited format)");
			throw new BankingException("Bank statements is in incorrect file format, Please use csv(comma delimited format)", 551,org_id,user_id);
		}
		bankMap.put("org_id", org_id);
		File convertedFile = new File(attchmntUrlPath+ bankFlatFile.getOriginalFilename());
		int row_no=0;
		int create_count =0;
		Map<String,Integer> map = new HashMap<>();
		map.put("record_count", row_no);
		map.put("create_count", create_count);
		try {
			Map<String, String> dateMap = reportFilterService.getDateRange("Today",null);
			Date date5 = new SimpleDateFormat("yyyy-MM-dd").parse(dateMap.get("from_date"));
			String formatString = sessionInfo.get("dateFormat");
			if (formatString.isEmpty()) {
				// Default Client PlugIn format
				formatString = "yyyy-MM-dd";
			}
			String dateString2 = new SimpleDateFormat(formatString).format(date5);
			//BufferedReader br = null;
			CSVReader reader = null;
			String line = "";
			String cvsSplitBy = ",";
			List<Map<String, String>> bankTransList = new ArrayList<Map<String, String>>();

			double bank_bal = Double.parseDouble(bankMap.get("bank_bal_amt"));
			//read bank account for get latest balance details
			BankAccount bankacc = new BankAccount();
			bankacc.setOrg_id(org_id);
			bankacc.setUser_id(user_id);
			bankacc.setBank_acct_id(bankMap.get("bank_acct_id"));
			bankacc.setDateFormat(formatString);
			bankacc.setBranch_id(branch_id);
			List<BankAccount> readBankList = readBankAccount(bankacc);
			if(readBankList.size()!= 0)
			{
				bank_bal = readBankList.get(0).getBank_bal_amt();
			}

			LOGGER.info("Flat file has no errors !");
			DecimalFormat df = new DecimalFormat("####0.00");
			reader = new CSVReader(new FileReader(convertedFile));
			String[] header = reader.readNext();
			//			br = new BufferedReader(new FileReader(convertedFile));
			//			br.readLine();
			row_no = 1;
			//			while ((line = br.readLine()) != null) {
			String[] bank;
			while ((bank = reader.readNext()) != null) {
				line = Arrays.toString(bank);
				row_no = row_no+1;
				try {
					String key = Integer.toString(row_no);
					//check the current row is perfect or not
					if(flat_file_map.containsKey(key) && flat_file_map.get(key) != null && !flat_file_map.get(key).equals("false"))
					{
						Map<String, String> bankTransMap = new HashMap<String, String>();
						bankTransMap.putAll(bankMap);
						// use comma as separator
						//						String[] bank = line.split(cvsSplitBy);
						
						//create bank trans table entry for this data
						if(bank.length!=0)
						{
							String dat = bank[0];
							if(dat != null)
							{
								dat = dat.replace("-","/");
								dat = dat.replace(".","/");
							}
							bank[0] = dat;
							bankTransMap.put("bank_trans_id", randomNumberUtil.getRandomNumber());
							//convert the date to store
							String dateFormat = sessionInfo.get("dateFormat");
							dateFormat = dateFormat.replace("-","/");
							bankTransMap.put("trans_dt", dateConversion.convertToStoreBankStmt(bank[0].toString(),dateFormat));
							bankTransMap.put("trans_txt", bank[1]);
							bankTransMap.put("reference_sn", bank[2]);
							bankTransMap.put("creation_dt", dateConversion.convertToStoreDate(dateString2,bankacc));
							double credit=0.00;
							double debit = 0.00;
							if(type != null && type.equals("Single"))//the amount is negative then its withdrawal, else its deposit
							{
								if(bank.length <=4)
								{
									if(bank[3].isEmpty())
									{
										bank[3]="0.0";
									}
									double amount = Double.parseDouble(bank[3]);
									if(amount < 0)//withdrawal
									{
										debit = amount;
									}
									else//deposit
									{
										credit = amount;
									}

									if(debit != 0)
									{
										credit = 0.00;
									}
									else
									{
										credit = amount;
									}
								}
							}
							else//get withdrawal and deposit amount detail from file
							{
								if(bank.length <=4)
								{
									credit = Double.parseDouble(bank[3]);
								}
								else
								{
									if (bank[4].isEmpty())
									{
										bank[4]="0.0";
									}

									debit = Double.parseDouble(bank[4]);
									if(debit != 0)
									{
										credit = 0.00;
									}
									else
									{
										credit = Double.parseDouble(bank[3]);
									}
								}
							}
							if (debit == 0) {
								bankTransMap.put("deb_cre_ind", "CR");
								bankTransMap.put("amount_amt", df.format(Math.abs(credit)));
								bank_bal = bank_bal + Math.abs(credit);
							} else {
								bankTransMap.put("deb_cre_ind", "DR");
								bankTransMap.put("amount_amt", df.format(Math.abs(debit)));
								bank_bal = bank_bal - Math.abs(debit);
							}
							bankTransMap.put("doc_id", bankTransMap.get("bank_trans_id"));
							bankTransMap.put("tableId", "114");
							bankTransMap.put("range_key", "doc_id");
							bankTransMap.put("org_id", bankMap.get("org_id"));
							bankTransMap.put("bank_acct_id", bankMap.get("bank_acct_id"));
							bankTransMap.put("branch_id", branch_id);
							bankTransList.add(bankTransMap);
						}
					}
				}catch (BankingException zge) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+zge ;
					LOGGER.info(message);
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason(zge.getMessage());
					record.setRecord(line);
					rejectList.add(record);
					continue;
				}  
				catch (ZetranGlobalException zge) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+zge ;
					LOGGER.info(message);
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason(zge.getMessage());
					record.setRecord(line);
					rejectList.add(record);
					continue;
				} catch (IOException ex) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
					LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason("Bank statement file has invalid data");
					record.setRecord(line);
					rejectList.add(record);
					continue;
				} 
				catch (Exception ex) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
					LOGGER.info(message);
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason(ex.getMessage());
					record.setRecord(line);
					rejectList.add(record);
					continue;
				} 

			}
			/** For Transaction Management **/

			TransObj transObj = new TransObj();
			transObj.addMapItemsForInsert(bankTransList, "114");
			create_count = bankTransList.size();
			if(mode != null && mode.equals("save"))
			{
				Commit comObj = new Commit();
				comObj.setOrg_id(org_id);
				comObj.setObj_id(bankMap.get("bank_acct_id"));
				comObj.setObj_typ("Bank"); 
				comObj.setBranch_id(branch_id);
				transObj.commitTransaction(comObj,bankacc,branch_id);

				LOGGER.info("All Transactions are Successfully Stored !");
				/* For Updating the Bank Balance in Bank Master table
				UpdateAttribute updateObj = new UpdateAttribute();
				updateObj.setBank_acct_id(bankMap.get("bank_acct_id").toString());
				updateObj.setOrg_id(bankMap.get("org_id"));
				updateObj.setColName("bank_bal_amt");
				updateObj.setColValue(Double.toString(bank_bal));
				updateObj.setRange_key("range_key");
				updateObj.setTableId("113");
				transObj.updateSingleAttribute(updateObj, "master");*/
				
				//update the balance details
				List<MybooksBalance> balanceList = new ArrayList<>();
				MybooksBalance balance = new MybooksBalance();
				balance.setOrg_id(org_id);
				balance.setBank_acct_id(bankMap.get("bank_acct_id").toString());
				balance.setBranch_id(branch_id);
				balance.setBank_balance(bank_bal);
				balanceList.add(balance);
				if(balanceList.size() != 0)
				{
					mybooksBalanceDao.saveMybooksBankBalance(balanceList);
				}

				//create Event
				BankAccount bankacc1 = new BankAccount();
				bankacc1.setOrg_id(org_id);
				bankacc1.setUser_id(user_id);
				bankacc1.setBank_acct_id(bankMap.get("bank_acct_id"));
				bankacc1.setBank_acct_nm(bankMap.get("bank_acct_nm"));
				bankacc1.setBranch_id(branch_id);
				eventService.bankReconcillationEvent(bankacc1,branch_id);
			}
			map.put("record_count", row_no-1);
			map.put("create_count", create_count);
			UploadStatus uploadStatus = newMassMigrationService.uploadStatus(org_id,user_id,"Bank",rejectList,map,mode,branch_id,doc_id,false);
			eventService.uploadFileStatusEvent(uploadStatus,branch_id);
		} catch (ZetranGlobalException me) {
			convertedFile.delete();
			map.put("record_count", row_no-1);
			map.put("create_count", create_count);
			UploadStatus uploadStatus = newMassMigrationService.uploadStatus(org_id,user_id,"Bank",rejectList,map,mode,branch_id,doc_id,false);
			eventService.uploadFileStatusEvent(uploadStatus,branch_id);
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			convertedFile.delete();
			map.put("record_count", row_no-1);
			map.put("create_count", create_count);
			UploadStatus uploadStatus = newMassMigrationService.uploadStatus(org_id,user_id,"Bank",rejectList,map,mode,branch_id,doc_id,false);
			eventService.uploadFileStatusEvent(uploadStatus,branch_id);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}


	/**
	 * flatFileValidation will validate the all flat file
	 * 
	 * @param bankMap
	 * @param bankFlatFile
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	private Map<String,String> flatFileValidation(Map<String, String> bankMap,MultipartFile bankFlatFile,List<RejectRecords> rejectList,String type) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String baseCountry = entity.getBaseCountry();
		if(bankFlatFile == null)
		{
			LOGGER.info("Bank statements is in incorrect file format, Please use csv(comma delimited format)");
			throw new BankingException("Bank statements is in incorrect file format, Please use csv(comma delimited format)", 551,org_id,user_id);
		}
		File convertedFileDuplicate = new File(attchmntUrlPath+ bankFlatFile.getOriginalFilename());
		if(convertedFileDuplicate.exists())
		{
			convertedFileDuplicate.delete();
		}
		File convertedFile = new File(attchmntUrlPath+ bankFlatFile.getOriginalFilename());
		int row_no = 1;
		int reject_count =0;
		try {
			bankFlatFile.transferTo(convertedFile);
			Map<String,String> map = new HashMap<>();
			String cvsSplitBy = ",";
			String vline = "";
			/*	BufferedReader hr = new BufferedReader(new FileReader(convertedFile));
			/** For Flat File Initial Validation **
			String headerLine = hr.readLine();
			String[] headerBank = headerLine.split(cvsSplitBy);
			/** Check the column fields validation **/
			CSVReader reader = new CSVReader(new FileReader(convertedFile));
			String[] headerBank = reader.readNext();
			if(type != null && type.equals("Single")) {
				if (headerBank.length < 4) {
					LOGGER.info("Bank statement file has invalid headers");
					throw new BankingException("Bank statement file has invalid headers", 553,org_id,user_id);
				}
			}
			else
			{
				if (headerBank.length < 5) {
					LOGGER.info("Bank statement file has invalid headers");
					throw new BankingException("Bank statement file has invalid headers", 553,org_id,user_id);
				}
			}
			String[] validate;
			Boolean date_val = false;
			//			while ((vline = hr.readLine()) != null) {
			while ((validate = reader.readNext()) != null) {
				vline = Arrays.toString(validate);
				try {
					Boolean flat_file_check = false;
					date_val=false;
					// For Transaction Date Validation
					boolean dateValidation = true;
					boolean totalDebitCreditValid = true;
					row_no = row_no+1;
					char c = '"';
					// use comma as separator
					//					String[] validate = vline.split(cvsSplitBy);
					for(String str: validate)//the validate contain special character ' this or not
					{
						if(str != null && str.contains(Character.toString(c)))
						{
							str = str.replace('"',' ');
							vline = vline.replace('"',' ');
							RejectRecords record = new RejectRecords();
							record.setRow_no(row_no);
							record.setReject_reason("Bank statement file has invalid data");
							record.setRecord(vline);
							//record.setRecord_data(vline);
							rejectList.add(record);
							totalDebitCreditValid = false;
							break;
						}
					}
					//check the amount fields
					if(validate.length!=0)
					{
						if(type != null && type.equals("Single")) {
							if(validate.length>4)
							{
								LOGGER.info("Bank statement file has invalid data");
								throw new BankingException("Bank statement file has invalid data", 552,org_id,user_id);
							}
						}
						else
						{
							if(validate.length>5)
							{
								LOGGER.info("Bank statement file has invalid data");
								throw new BankingException("Bank statement file has invalid data", 552,org_id,user_id);
							}
						}
						//validate the date format
						String sDate = "";
						String dat = validate[0];
						if(dat != null)
						{
							dat = dat.replace("-","/");
							dat = dat.replace(".","/");
						}
						validate[0] = dat;
						String orgDate = sessionUtil.getSessionValue("dateFormat");
						orgDate = orgDate.replace("-","/");
						String dateFormat = dateConversion.getBankFileDateFormat(validate[0],"/",orgDate);
						date_val=true;
						SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
						Date transDate = sdf.parse(validate[0]);
						sDate = sdf.format(transDate);

						if (!(sDate.toString().equals(validate[0].toString())))
						{
							RejectRecords record = new RejectRecords();
							record.setRow_no(row_no);
							record.setReject_reason("Bank statement file has invalid date format "+validate[0]);
							record.setRecord(vline);
							rejectList.add(record);
							break;
							//						LOGGER.info("Bank statement file has invalid date format "+validate[0]);
							//						throw new BankingException("Bank statement file has invalid date format "+validate[0], 556,org_id,user_id);
						}
						dateValidation = dateValidation
								&& ((sDate.toString().equals(validate[0].toString())));

						// For Debit and Credit column Validation
						if(type != null && type.equals("Single")) {
							if(validate.length <=2)
							{
								RejectRecords record = new RejectRecords();
								record.setRow_no(row_no);
								record.setReject_reason("Bank statement file has invalid deposits and withdrawal amount column.");
								record.setRecord(vline);
								rejectList.add(record);
								totalDebitCreditValid = false;
								break;
								//						LOGGER.info("Bank statement file has invalid deposits and withdrawal columns. row_no- "+row_no);
								//						throw new BankingException("Bank statement file has invalid data.row_no- "+row_no,552,org_id,user_id);			
							}
						}
						else
						{
							if(validate.length <=3)
							{
								RejectRecords record = new RejectRecords();
								record.setRow_no(row_no);
								record.setReject_reason("Bank statement file has invalid deposits and withdrawal columns.");
								record.setRecord(vline);
								rejectList.add(record);
								totalDebitCreditValid = false;
								break;
								//						LOGGER.info("Bank statement file has invalid deposits and withdrawal columns. row_no- "+row_no);
								//						throw new BankingException("Bank statement file has invalid data.row_no- "+row_no,552,org_id,user_id);			
							}
						}
						double CreditValidAmount=0.00;
						double DebitValidAmount=0.00;
						if(type != null && type.equals("Single")) {//the amount is negative then its withdrawal, else its deposit
							if(validate.length <=4)
							{
								if(validate[3].isEmpty())
								{
									validate[3]="0.0";
								}
								double amount = Double.parseDouble(validate[3]);
								if(amount < 0)//withdrawal
								{
									DebitValidAmount = amount;
								}
								else//deposit
								{
									CreditValidAmount = amount;
								}
								if (DebitValidAmount != 0 )
								{
									CreditValidAmount = 0.00;
								}
								else
								{
									CreditValidAmount = amount;
								}
							}
						}
						else
						{
							if(validate.length <=4)
							{
								if (validate[3].isEmpty())
								{
									validate[3]="0.0";
								}
								CreditValidAmount = Double.parseDouble(validate[3]);
							}
							else
							{
								if (validate[4].isEmpty())
								{
									validate[4]="0.0";
								}
								if (validate[3].isEmpty())
								{
									validate[3]="0.0";
								}
								if(Double.parseDouble(validate[3]) >0 && Double.parseDouble(validate[4]) >0 )
								{
									RejectRecords record = new RejectRecords();
									record.setRow_no(row_no);
									record.setReject_reason("Invalid amount format,either enter it in deposit or withdrawal column");
									record.setRecord(vline);
									rejectList.add(record);
									totalDebitCreditValid = false;
									break;
									//							LOGGER.info("Invalid amount format,either enter it in deposit or withdrawal column. row no - "+row_no);
									//							throw new BankingException("Invalid amount format,either enter it in deposit or withdrawal column. row no - "+row_no,559,org_id,user_id);
								}
								DebitValidAmount = Double.parseDouble(validate[4]);
								if (DebitValidAmount != 0 )
								{
									CreditValidAmount = 0.00;
								}
								else
								{
									CreditValidAmount = Double.parseDouble(validate[3]);
								}
							}
						}
						if(DebitValidAmount == 0 && CreditValidAmount == 0)
						{
							RejectRecords record = new RejectRecords();
							record.setRow_no(row_no);
							record.setReject_reason("Both deposit and withdrawal values should not be 0");
							record.setRecord(vline);
							rejectList.add(record);
							totalDebitCreditValid = false;
							break;
							//						LOGGER.info("Both deposit and withdrawal columns should not be 0. row_no- "+row_no);
							//						throw new BankingException("Both deposit and withdrawal columns should not be 0. row_no- "+row_no,560,org_id,user_id);			
						}
					}
					//save the row based validation in map
					flat_file_check = dateValidation && totalDebitCreditValid;
					if(!flat_file_check)
					{
						reject_count = reject_count +1;
					}
					map.put(Integer.toString(row_no), Boolean.toString(flat_file_check));
				}
				catch (BankingException zge) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+zge ;
					LOGGER.info(message);
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason(zge.getMessage());
					record.setRecord(vline);
					rejectList.add(record);
					reject_count = reject_count +1;
					map.put(Integer.toString(row_no), Boolean.toString(false));
					continue;
				} 
				catch (NumberFormatException ex) {
					if(!date_val)
					{
						RejectRecords record = new RejectRecords();
						record.setRow_no(row_no);
						record.setReject_reason("Bank statement file has invalid date format");
						record.setRecord(vline);
						rejectList.add(record);
						reject_count = reject_count +1;
						map.put(Integer.toString(row_no), Boolean.toString(false));
					}
					else
					{
						RejectRecords record = new RejectRecords();
						record.setRow_no(row_no);
						record.setReject_reason("Bank statement file has invalid deposits and withdrawal columns,please use numbers only");
						record.setRecord(vline);
						rejectList.add(record);
						reject_count = reject_count +1;
						map.put(Integer.toString(row_no), Boolean.toString(false));
					}
					continue;
					//					LOGGER.info("Bank statement file has invalid deposits and withdrawal columns,please use numbers only. row_no -"+row_no);
					//					throw new BankingException("Bank statement file has invalid deposits and withdrawal columns,please use numbers only. row_no -"+row_no,558,org_id,user_id);	
				}  
				catch (Exception ex) {
					String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
					LOGGER.info(message);
					RejectRecords record = new RejectRecords();
					record.setRow_no(row_no);
					record.setReject_reason(ex.getMessage());
					record.setRecord(vline);
					rejectList.add(record);
					reject_count = reject_count +1;
					map.put(Integer.toString(row_no), Boolean.toString(false));
					continue;
				}
			}

			map.put("total_count", Integer.toString(row_no-1));
			map.put("reject_count", Integer.toString(reject_count));
			//			map.put("flat_file_check",Boolean.toString(flat_file_check));
			return map;
		}
		catch (BankingException zge) {
			convertedFile.delete();
			errorLogService.saveErrorLogInfo(zge);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+zge ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message);
			throw zge;
		} 
		catch (Exception ex) {
			convertedFile.delete();
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * get mybooks balance based on transaction
	 * @param bankList
	 * @return
	 * @throws Exception
	 */

	public List<BankAccount> getMybooksBalance(List<BankAccount> bankList,String branch_id) throws Exception
	{
		List<String> acct_ids = new ArrayList<>();
		for (int bank = 0; bank < bankList.size(); bank++) {
			if (bankList.get(bank).getBank_acct_id() != null) {
				acct_ids.add(bankList.get(bank).getBank_acct_id());
			}
		}

		Map<String,String> transMap = new HashMap<>();
		ObjectMapper mapper = new ObjectMapper();
		String str = mapper.writeValueAsString(acct_ids);
		transMap.put("acct_id", str);
		transMap.put("daterange","Today");
		transMap.put("cashAccrualType", "accrual");
		transMap.put("reportType", "trialBal");
		//get gl transaction details
		String glReport=commonGLReportService.readGLReport(transMap,"trialBalance",branch_id);

		Map<String,String> glObject = mapper.readValue(glReport,new TypeReference<HashMap<String,String>>() { });//convert the input data into map object using mapper
		List<Map<String,String>> reportArray=mapper.readValue(glObject.get("reportList"),
				TypeFactory.defaultInstance().constructCollectionType(List.class,  
						Map.class));
		//get the transaction amount for each bank acct id for mybooks balance
		for (int bank = 0; bank < bankList.size(); bank++) {
			if (bankList.get(bank).getBank_acct_id() != null) {
				Map<String,String> map = GLBalanceSheetService.getAmount(reportArray, "Asset", bankList.get(bank).getBank_acct_id(), null);
				if(map.containsKey("amount_amt"))
				{
					bankList.get(bank).setMybooks_bal_amt(Double.parseDouble(map.get("amount_amt")));
				}
				else
				{
					bankList.get(bank).setMybooks_bal_amt(0.0);
				}
			}
		}
		return bankList;
	}

	/**
	 * read the list of bank account information
	 * @return
	 * @throws Exception
	 */
	public String manageBankAccounts()throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			ReadAttribute readObj = new ReadAttribute();
			readObj.setOrg_id(org_id);
			readObj.setTableId("113");
			Map<String,String> bankFilter = new HashMap<>();
			Map<String,String> fieldMap = new HashMap<>();
			/*if(branch_id != null && !branch_id.isEmpty())
			{
				String filter_expr = "";
				if(branch_id.equals("HeadQuarters"))
				{
					filter_expr = "((branch_id=:v_value1) or attribute_not_exists(branch_id))";
				}
				else
				{
					filter_expr = "(branch_id=:v_value1)";
				}
				readObj.setFilter(filter_expr);
				bankFilter.put("filterVal1",branch_id);
				fieldMap.put("branch_id",null);
			}*/
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			List<BankAccount> bankList = (List<BankAccount>)transObj.readTrans(readObj,bankFilter,fieldMap,"master","com.zetran.acct.pojo.BankAccount","","master",null);

			Map<String, String> accMap=new HashMap<>();
			Map<String, String> accFilter=new HashMap<>();
			accMap.put("org_id", org_id);
			List<COAAccount> accList = accountService.readMultipleAccount(accMap, accFilter,fieldMap);
			Map<String,String> acctCodeMap = new HashMap<>();
			for(COAAccount acc : accList)
			{
				acctCodeMap.put(acc.getAcct_id(), acc.getAcct_code());
			}
			// For Getting Base Currency Symbol
			String curr_sym = currencyService.getBaseCurrencySymbol();
			bankList = getMybooksBalance(bankList,branch_id);
			//getMybooks balance 
			List<MybooksBalance> balanceList = mybooksBalanceDao.getMybooksBalanceList(org_id,branch_id);
			if(balanceList == null || (balanceList != null && balanceList.size() ==0))
			{
				//save the bank and mybooks balance in table
				List<MybooksBalance> balanceList_save = new ArrayList<>();
				for(BankAccount acc : bankList)
				{
					MybooksBalance balance = new MybooksBalance();
					balance.setOrg_id(org_id);
					balance.setBank_acct_id(acc.getBank_acct_id());
					balance.setBranch_id(branch_id);
					balance.setBank_balance(acc.getBank_bal_amt());
					balance.setMybooks_balance(acc.getMybooks_bal_amt());
					balanceList_save.add(balance);
				}
				if(balanceList_save.size() != 0)
				{
					mybooksBalanceDao.saveBalance(balanceList_save);
				}
				balanceList = mybooksBalanceDao.getMybooksBalanceList(org_id,branch_id);
			}
			//set balance info in map based on bank acct id
			Map<String,Double> mybooksMap = new HashMap<>();
			Map<String,Double> bankMap = new HashMap<>();
			if(balanceList != null)
			{
				for(MybooksBalance mybooks : balanceList)
				{
					mybooksMap.put(mybooks.getBank_acct_id(), mybooks.getMybooks_balance());
					bankMap.put(mybooks.getBank_acct_id(), mybooks.getBank_balance());
				}
			}
			ObjectMapper mapper = new ObjectMapper();
			List<BankAccount> list = new ArrayList<>();
			for (int bank = 0; bank < bankList.size(); bank++) {
				bankList.get(bank).setAcct_code(acctCodeMap.get(bankList.get(bank).getBank_acct_id()));
				/**
				 * Putting the bank feed flag and removing the bank_security_key
				 **/
				if (bankList.get(bank).getBank_item_id() != null
						&& bankList.get(bank).getBank_security_key() != null && bankList.get(bank).getBank_item_id().equals("null") &&
						bankList.get(bank).getBank_security_key().equals("null")) {
					bankList.get(bank).setBank_security_key(null);
					bankList.get(bank).setBank_item_id(null);
					bankList.get(bank).setInstitute_name(null);
				}
				if (bankList.get(bank).getBank_item_id() != null
						&& bankList.get(bank).getBank_security_key() != null) {
					// putting bank_feed_flg as true
					bankList.get(bank).setBank_feeds_flg(true);
					// removing the 'bank_security_key', no need to pass
					// confidential info to client
					bankList.get(bank).setBank_security_key(null);
				}
				if(bankList.get(bank).getInstitute_name() != null && bankList.get(bank).getInstitute_name().equals("ICICI"))
				{
					// putting bank_feed_flg as true
					bankList.get(bank).setBank_feeds_flg(true);
				}
				//convert to display the date
				if (bankList.get(bank).getDoc_dt() != null && !bankList.get(bank).getDoc_dt().isEmpty()) {
					bankList.get(bank).setDoc_dt(dateConversion.convertToDisplayinSession(bankList.get(bank).getDoc_dt()));	
				}
				bankList.get(bank).setCurr_sym(curr_sym);
				//set latest balance information
				if(bankMap.get(bankList.get(bank).getBank_acct_id()) != null)
				{
					bankList.get(bank).setBank_bal_amt(Precision.round(bankMap.get(bankList.get(bank).getBank_acct_id()), 2));
				}
				if(mybooksMap.get(bankList.get(bank).getBank_acct_id()) != null)
				{
					bankList.get(bank).setMybooks_bal_amt(Precision.round(mybooksMap.get(bankList.get(bank).getBank_acct_id()), 2));
				}
				if(bankList.get(bank).getAcct_cat() == null || bankList.get(bank).getAcct_cat()!= null && bankList.get(bank).getAcct_cat().isEmpty())
				{
					bankList.get(bank).setAcct_cat("Cash");
				}
				list.add(bankList.get(bank));
			}
			//group the bank accounts based on the bank item id
			Map<String,List<BankAccount>> map = new HashMap<>();
			JSONArray array = new JSONArray();
			for(BankAccount bank : list)
			{
				if(bank.getBank_item_id() != null && !bank.getBank_item_id().isEmpty())
				{
					if(map.get(bank.getBank_item_id()) != null)
					{
						List<BankAccount> bList = map.get(bank.getBank_item_id());
						bList.add(bank);
						map.put(bank.getBank_item_id(),bList);
					}
					else
					{
						List<BankAccount> bList = new ArrayList<>();
						bList.add(bank);
						map.put(bank.getBank_item_id(),bList);
					}
				}
				else
				{
					String str = mapper.writeValueAsString(bank);
					JSONObject json = new JSONObject(str);
					array.put(json);
				}
			}

			//if the institute name is empty the set the default institute name for display
			int count =1;
			for(String bank_item_id : map.keySet())
			{
				JSONObject json = new JSONObject();
				json.put("bank_item_id",bank_item_id);
				List<BankAccount> blist = map.get(bank_item_id);
				String institute_name = blist.get(0).getInstitute_name();
				if(institute_name == null || (institute_name != null && institute_name.isEmpty()))
				{
					institute_name = "BankAccount-"+count;
					count++;
				}
				json.put("institute_name",institute_name);
				String str = mapper.writeValueAsString(blist);
				JSONArray bankFeeds = new JSONArray(str);
				json.put("bankFeeds",bankFeeds);
				array.put(json);
			}
			LOGGER.info("Successfully Retrieved the multiple Bank Accounts !");
			//update mybooks balance 
			updateMyBooksBalance(list);
			return array.toString();
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * delete the plaid bank feeds account
	 * @param json
	 * @return
	 * @throws Exception
	 */
	public String deleteBankFeedsAccount(JSONObject json) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			PlaidClient plaidClient = plaidConnecter.getPlaidConnection();
			String bank_item_id = json.optString("bank_item_id");
			if(bank_item_id != null && !bank_item_id.isEmpty())
			{
				//get bank item id based list of bank accounts
				Map<String,String> bankMap = new HashMap<>();
				bankMap.put("org_id", org_id);
				bankMap.put("tableId", "113");
				String filterExpression = ("(bank_item_id=:v_value1)");
				bankMap.put("filterExpression", filterExpression);
				Map<String, String> bank_filter = new HashMap<String, String>();
				bank_filter.put("filterVal1", bank_item_id);
				Map<String,String> fieldMap = new HashMap<>();
				fieldMap.put("bank_item_id",null);
				TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
				List<Map<String,String>> bankList = transObj.readMultiple(bankMap, bank_filter,fieldMap, "master",null);

				//delete the plaid bank feed account and coa account
				for (int i = 0; i < bankList.size(); i++) {
					if (bankList.get(i).containsKey("bank_item_id")
							&& bankList.get(i).containsKey("bank_security_key") && bankList.get(i).get("bank_item_id") != null
							&& bankList.get(i).get("bank_security_key") != null && !bankList.get(i).get("bank_item_id").equals("null") && 
							!bankList.get(i).get("bank_security_key").equals("null")) {
						if(!bankList.get(i).get("active_flg").equals("F"))
						{
							Response<ItemDeleteResponse> response = plaidClient.service().itemDelete(new ItemDeleteRequest(bankList.get(i).get("bank_security_key"))).execute();
							bankTransactionService.updateBankFlag(bankMap.get("org_id"),bankList.get(i).get("bank_acct_id"));
							COAAccount account = new COAAccount();
							account.setOrg_id(org_id);
							account.setAcct_id(bankList.get(i).get("bank_acct_id"));
							accountService.deleteAccount(account,branch_id);
							if (response.isSuccessful()) {
								LOGGER.info("Plaid Account is deleted !");
							}
						}
					}
				}
			}
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * Refresh the bank balance based on the transaction
	 * @param bankObj
	 * @param org_id
	 * @param user_id
	 * @return
	 * @throws Exception
	 */

	public String refreshBankBalance(JSONObject bankObj,String org_id,String user_id,String branch_id)
			throws Exception {
		try {
			//bankingUtil.bankBalanceCalculation(bankObj.getString("bank_acct_id"), null, org_id, user_id);
			
			//get bank transaction based on bank acct id
			Map<String,String> bankMap = new HashMap<>();
			bankMap.put("org_id", org_id);
			bankMap.put("tableId", "114");
			String filterExpression = ("(bank_acct_id=:v_value1)");
			bankMap.put("filterExpression", filterExpression);
			Map<String, String> bank_filter = new HashMap<String, String>();
			bank_filter.put("filterVal1", bankObj.optString("bank_acct_id"));
			Map<String,String> fieldMap = new HashMap<>();
			fieldMap.put("bank_acct_id",null);
			if(branch_id != null && !branch_id.isEmpty())
			{
				if(branch_id.equals("HeadQuarters"))
				{
					filterExpression = filterExpression +" and ((branch_id=:v_value2) or attribute_not_exists(branch_id))";
				}
				else
				{
					filterExpression = filterExpression +" and (branch_id=:v_value2)";
				}
				bankMap.put("filterExpression", filterExpression);
				bank_filter.put("filterVal2",branch_id);
				fieldMap.put("branch_id",null);
			}
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			List<Map<String,String>> transList = transObj.readMultiple(bankMap, bank_filter,fieldMap, "transaction",null);

			//sort all the data in date based ascending order
			entity.sortMapList("trans_dt", transList);

			double bank_balance = 0;
			for(Map<String,String> map : transList)
			{
				if(map.containsKey("deb_cre_ind") && map.get("deb_cre_ind") != null && map.get("deb_cre_ind").equals("DR"))
				{
					double withdraw = Double.parseDouble(map.get("amount_amt"));
					bank_balance = bank_balance-withdraw;
				}
				else
				{
					double deposit = Double.parseDouble(map.get("amount_amt"));
					bank_balance = bank_balance+deposit;	
				}
			}

			//getBank Account opening balance details
			BankAccount bankacc = new BankAccount();
			bankacc.setOrg_id(org_id);
			bankacc.setUser_id(user_id);
			bankacc.setBank_acct_id(bankObj.optString("bank_acct_id"));
			bankacc.setBranch_id(branch_id);
			List<BankAccount> baList = readBankAccount(bankacc);
			BankAccount ba = null;
			if(baList != null && baList.size() !=0)
			{
				ba = baList.get(0);
				bank_balance = bank_balance + ba.getAmount_amt();
			}

			//check opening balance also
			bank_balance = openingBalanceService.refreshBankBalanceInOpenbal(org_id, bank_balance, bankObj.optString("bank_acct_id"),branch_id);

			/*update bank_balance
			UpdateAttribute updateObj = new UpdateAttribute();
			updateObj.setBank_acct_id(bankObj.optString("bank_acct_id"));
			updateObj.setOrg_id(org_id);
			updateObj.setColName("bank_bal_amt");
			updateObj.setColValue(Double.toString(bank_balance));
			updateObj.setRange_key("range_key");
			updateObj.setTableId("113");
			transObj.updateSingleAttribute(updateObj, "master");	*/
			List<MybooksBalance> balanceList = new ArrayList<>();
			MybooksBalance balance = new MybooksBalance();
			balance.setOrg_id(org_id);
			balance.setBank_acct_id(bankObj.optString("bank_acct_id"));
			balance.setBranch_id(branch_id);
			balance.setBank_balance(bank_balance);
			balanceList.add(balance);
			if(balanceList.size() != 0)
			{
				mybooksBalanceDao.saveMybooksBankBalance(balanceList);
			}
		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * disable bank feeds account remove the bank security related information for this account
	 * @param json
	 * @return
	 * @throws Exception
	 */
	public String disableBankFeedsAccount(JSONObject json) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try {
			PlaidClient plaidClient = plaidConnecter.getPlaidConnection();
			String bank_item_id = json.optString("bank_item_id");
			if(bank_item_id != null && !bank_item_id.isEmpty())
			{
				//get bank item id basis list of bank accounts
				Map<String,String> bankMap = new HashMap<>();
				bankMap.put("org_id", org_id);
				bankMap.put("tableId", "113");
				String filterExpression = ("(bank_item_id=:v_value1)");
				bankMap.put("filterExpression", filterExpression);
				Map<String, String> bank_filter = new HashMap<String, String>();
				bank_filter.put("filterVal1", bank_item_id);
				Map<String,String> fieldMap = new HashMap<>();
				fieldMap.put("bank_item_id",null);
				TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
				List<Map<String,String>> bankList = transObj.readMultiple(bankMap, bank_filter,fieldMap, "master",null);
				List<UpdateAttribute> list = new ArrayList<>();
				//delete the bank feeds account in plaid
				for (int i = 0; i < bankList.size(); i++) {
					if (bankList.get(i).containsKey("bank_item_id")
							&& bankList.get(i).containsKey("bank_security_key") && bankList.get(i).get("bank_item_id") != null
							&& bankList.get(i).get("bank_security_key") != null && !bankList.get(i).get("bank_item_id").equals("null") && 
							!bankList.get(i).get("bank_security_key").equals("null")) {
						if(!bankList.get(i).get("active_flg").equals("F"))
						{
							Response<ItemDeleteResponse> response = plaidClient.service().itemDelete(new ItemDeleteRequest(bankList.get(i).get("bank_security_key"))).execute();
							//bankTransactionService.updateBankFlag(bankMap.get("org_id"),bankList.get(i).get("bank_acct_id"));
							
							//remove the plaid specific fields in banking for display normal bank account
							UpdateAttribute updateObj1 = new UpdateAttribute();
							updateObj1.setBank_acct_id(bankList.get(i).get("bank_acct_id"));
							updateObj1.setOrg_id(org_id);
							updateObj1.setColName1("bank_item_id");
							updateObj1.setColValue1("null");
							updateObj1.setColName2("bank_security_key");
							updateObj1.setColValue2("null");
							updateObj1.setColName3("institute_name");
							updateObj1.setColValue3("null");

							updateObj1.setOrg_id(org_id);
							updateObj1.setRange_key("range_key");
							updateObj1.setTableId("113");
							list.add(updateObj1);
							if (response.isSuccessful()) {
								LOGGER.info("Plaid Account is deleted !");
							}
						}
					}
				}
				transObj.updateMultipleAttribute(list, "master");
			}

		} catch (ZetranGlobalException me) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * create account opening balance for bank account
	 * @param org_id
	 * @param user_id
	 * @param bankObj
	 * @return
	 * @throws Exception
	 */
	public Thread accountOpeningBalance(String org_id,String user_id,Map<String,Object> bankObj,String branch_id)throws Exception
	{
		Thread t1 = null;
		//create journal and set ref_doc_id
		Double amount_amt = Double.parseDouble(bankObj.get("amount_amt").toString());
		Journal journal = new Journal();
		List<Map<String,String>> journalItems = new ArrayList<>();
		journal.setOrg_id(org_id);
		journal.setUser_id(user_id);
		journal.setCashAccrualType("cash");
		journal.setDebittotal(amount_amt);
		journal.setCredittotal(amount_amt);
		journal.setDoc_dt(bankObj.get("doc_dt").toString());
		journal.setDoc_typ("Manual Journal");
		journal.setRef_doc_typ("Account Opening Balance");
		journal.setRef_doc_id(bankObj.get("bank_acct_id").toString());
		journal.setReference_sn("account opening balance");
		journal.setBranch_id(branch_id);
		Map<String,String> accountOb = new HashMap<>();
		// Getting the required Accounts
		String sysAccCodes[]={"OBA"};
		accountOb = dashboardUtil.getCOADetails(sysAccCodes, org_id,branch_id);
		String oba_account=accountOb.get("OBA");
		String acct_id = bankObj.get("bank_acct_id").toString();
		DecimalFormat df = new DecimalFormat("###.##");
		Map<String,String> accRec=new HashMap<>();
		/***********  Opening Balance Adjustments credit  ***********/
		if(oba_account!= null && !oba_account.isEmpty())
		{
			accRec.put("acct_id",oba_account);    // Getting the Account id
			accRec.put("debit","0.00");
			accRec.put("credit",df.format(amount_amt));
			if(!((accRec.get("debit").equals("0.00") || accRec.get("debit").equals("0.0")) &&(accRec.get("credit").equals("0.00") || accRec.get("credit").equals("0.0"))))
			{
				journalItems.add(accRec);
			}
		}
		/*********** Account debit ***********/
		if(acct_id != null && !acct_id.isEmpty())
		{
			Map<String,String> invAss=new HashMap<>();
			invAss.put("acct_id",acct_id);    // Getting the Account id
			invAss.put("credit","0.00");
			invAss.put("debit",df.format(amount_amt));
			if(!((invAss.get("debit").equals("0.00") || invAss.get("debit").equals("0.0")) &&(invAss.get("credit").equals("0.00") || invAss.get("credit").equals("0.0"))))
			{
				journalItems.add(invAss);
			}
		}
		if(journalItems.size() != 0)
		{
			t1 = manualJournalService.createJournal(journal, journalItems, null);
			myBooksBalanceUpdate(journalItems, "create");
			bankObj.put("ref_doc_id",journal.getDoc_id());
		}
		//convert to doc_dt for store
		String doc_dt = dateConversion.convertToStore(bankObj.get("doc_dt").toString());
		bankObj.put("doc_dt", doc_dt);
		return t1;
	}

	/**
	 * update the account opening balance amount value .need to change in future in branch specific
	 * @param bank_acct_id
	 * @param amount_amt
	 * @return
	 * @throws Exception
	 */
	public String updateOpeningBalance(String bank_acct_id,double amount_amt) throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			UpdateAttribute updateObj = new UpdateAttribute();
			updateObj.setBank_acct_id(bank_acct_id);
			updateObj.setOrg_id(org_id);
			updateObj.setColName("amount_amt");
			updateObj.setColValue(Double.toString(amount_amt));
			updateObj.setRange_key("range_key");
			updateObj.setTableId("113");
			TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
			transObj.updateSingleAttribute(updateObj, "master");
			JSONObject bankObj = new JSONObject();
			bankObj.put("bank_acct_id", bank_acct_id);
			refreshBankBalance(bankObj,org_id,user_id,branch_id);
			return "Opening Balance Updated";
		} catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
	}

	/**
	 * add icici bank feed account based on user credentials
	 * @return
	 * @throws Exception
	 */
	public String addIciciBankFeedAccount() throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			//get the icici registration details for get the account no
			CommonTable ct = new CommonTable();
			ct.setOrg_id(org_id);
			ct.setUser_id(user_id);
			ct.setPrimary_key("Icici Registration Details");
			CommonTable ctable = commonTableService.readCommonTable(org_id,user_id,ct);
			if(ctable != null)
			{
				JSONObject datajson = new JSONObject(ctable.getField1());
				String bank_acct_id = datajson.optString("account_no");
				//check this account id already available or not
				BankAccount ba = new BankAccount();
				ba.setOrg_id(org_id);
				ba.setUser_id(user_id);
				ba.setBank_acct_id(bank_acct_id);
				ba.setBranch_id(branch_id);
				List<BankAccount> list = readBankAccount(ba);
				
				if(list == null || list != null && list.size() == 0)
				{
					//get last 90 transaction information for this account
					SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
					Date to_dt = new Date();
					Date from_dt= transactionUtil.getDate(to_dt,90);
					String from_date = dateFormat.format(from_dt);
					String to_date = dateFormat.format(to_dt);

					JSONObject inputjson = new JSONObject();
					inputjson.put("AGGRID",datajson.optString("AGGR_ID"));
					inputjson.put("CORPID",datajson.optString("CORP_ID"));
					inputjson.put("USERID",datajson.optString("USER_ID"));
					inputjson.put("URN",datajson.optString("URN"));
					inputjson.put("ACCOUNTNO",datajson.optString("account_no"));
					inputjson.put("FROMDATE",from_date);
					inputjson.put("TODATE",to_date);

					//String data = "{\"AGGRID\":\"AGGRID\",\"CORPID\":\"CIBNEXT\",\"USERID\":\"CIBTESTING6\",\"URN\":\"URN\",\"ACCOUNTNO\":\"000405001611\",\"FROMDATE\":\"01-01-2016\",\"TODATE\":\"30-12-2016\"}";
					String input = Encryption.encrypt(inputjson.toString());
					//System.out.println("Encrypted : "+input);

					//					String host = "api.icicibank.com:8443";
					//					String url = "https://api.icicibank.com:8443/api/Corporate/CIB/v1/AccountStatement";
					String host = "apibankingone.icicibank.com";
					String url = "https://apibankingone.icicibank.com/api/Corporate/CIB/v1/AccountStatement";
					String apikey = "6a878a6a650b4f928dbe91c7451f65cf";
					String x_forwarded_for = "54.161.37.23";
					if(datajson.optString("uat").equals("true"))
					{
						apikey = "3ad3cee2-c09b-40ad-92ed-d85f994ad2ea";
						url = "https://apigwuat.icicibank.com:8443/api/Corporate/CIB/v1/AccountStatement";
						host = "apigwuat.icicibank.com:8443";
					}

					HttpResponse<String> response = Unirest.post(url)
							.header("Accept", "*/*")
							.header("Content-Type","text/plain")
							.header("apikey",apikey)
							.header("Host",host)
							.header("x-forwarded-for",x_forwarded_for)
							.body(input)
							.asString();
					//System.out.println(response.getBody());
					String message = "Org_id: " +org_id +"  User_id: "+user_id+"  Account Statement output: "+response.getBody();
					LOGGER.log(Level.INFO, message);//write the message in log file in info level
					if(response.getBody() != null)
					{
						//check the response
						JSONObject json = new JSONObject(response.getBody());
						if(!json.optString("encryptedKey").isEmpty())
						{
							//this is common steps for get account details from icici
							String decrypt1=json.optString("encryptedKey");
							String decrypt3 = Encryption.decrypt(decrypt1);
							//System.out.println(decrypt3);
							String decrypt2=json.optString("encryptedData");
							String output = Encryption.decryptByAES(decrypt3, decrypt2);
							//System.out.println(output);
							message = "Org_id: " +org_id +"  User_id: "+user_id+"  Account Statement output: "+output;
							LOGGER.log(Level.INFO, message);//write the message in log file in info level
							JSONObject result = new JSONObject(output);
							
							//check the response
							if(result.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("MESSAGE"),561);
							}
							if(result.optString("Response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("Message"),561);
							}
							if(result.optString("response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("message"),561);
							}
							if(result.optString("RESPONSE").equalsIgnoreCase("SUCCESS"))
							{
								//check the record is json object or json array
								String data = result.optString("Record");
								JSONArray array = new JSONArray();
								Object jsonin = new JSONTokener(data).nextValue();
								if (jsonin instanceof JSONObject)//if the result is json object then convert the data into json object
								{
									JSONObject object = result.optJSONObject("Record");
									array.put(object);
								}
								else if (jsonin instanceof JSONArray)//if the result is json array then convert the data into json array
								{
									array = result.optJSONArray("Record");
								}
								
								//check the record available or not based on create icici account
								if(array != null && array.length() != 0)
								{
									//calculate bank bal amt
									String amt = array.optJSONObject(array.length() - 1).optString("BALANCE");
									amt = amt.replaceAll(",", "");
									double bank_bal_amt = Double.parseDouble(amt);
									Map<String, String> bankFeedMap = new HashMap<String, String>();
									bankFeedMap.put("org_id", org_id);
									bankFeedMap.put("bank_acct_id",bank_acct_id);
									bankFeedMap.put("active_flg", "T"); // Make default account as active
									bankFeedMap.put("bank_security_key", "");
									bankFeedMap.put("bank_item_id", "");
									bankFeedMap.put("bank_bal_amt", Double.toString(bank_bal_amt));
									// Bank Feeds Time Stamp
									Timestamp creationDate = new Timestamp(System.currentTimeMillis());
									bankFeedMap.put("bankfeed_timestamp", creationDate.toString());

									bankFeedMap.put("acct_id",bank_acct_id);
									bankFeedMap.put("acct_typ", "Asset");
									bankFeedMap.put("acct_cat", "Cash");
									bankFeedMap.put("acct_nm_sn", "Icici Bank Account");
									// Passing the acct_typ and initial acct code to get the unduplicated acct_code
									bankFeedMap.put("acct_code", accountService.generateAccountCode("Asset", 1000));
									bankFeedMap.put("acct_desc", "Icici Bank Account");
									bankFeedMap.put("deposit_acct_typ", "Cash");

									bankFeedMap.put("institute_name","ICICI");
									bankFeedMap.put("branch_id",branch_id);
									JSONObject bankObj1 = new JSONObject(bankFeedMap);
									ObjectMapper mapper = new ObjectMapper();
									COAAccount account = mapper.readValue(bankObj1.toString(),COAAccount.class);
									accountService.createAccount(account);//create coa and bank account
									
									//create event 
									BankAccount bankacc = new BankAccount();
									bankacc.setOrg_id(org_id);
									bankacc.setUser_id(user_id);
									bankacc.setBank_acct_id(bank_acct_id);
									bankacc.setBank_acct_nm("Icici Bank Account");
									bankacc.setBranch_id(branch_id);
									eventService.bankReconcillationEvent(bankacc,branch_id);
									
									// Calling the refresh Bank Feeds
									JSONObject bankFeeds=new JSONObject();
									bankFeeds.put("bank_acct_id",bank_acct_id);
									refreshIciciBankFeedsByAccount(bankFeeds,array);
								}
							}
						}
						else//if the reponse failure then throw error
						{
							if(json.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("MESSAGE"),561);
							}
							if(json.optString("Response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("Message"),561);
							}
							if(json.optString("response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("message"),561);
							}
						}
					}
				}
			}
		}
		catch (ZetranGlobalException me) {
			errorLogService.saveErrorLogInfo(me);
			subscriptionService.updateBankFeedCount(org_id,"bankFeeds",branch_id);
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+me ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw me;
		} catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			subscriptionService.updateBankFeedCount(org_id,"bankFeeds",branch_id);
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * create icici bank feeds transaction 
	 * @param bankMap
	 * @param array
	 * @return
	 * @throws Exception
	 */
	public String refreshIciciBankFeedsByAccount(JSONObject bankMap,JSONArray array)
			throws Exception {
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		String branch_id = entity.getBranchId();
		try {
			Map<String,String> dateRange = reportFilterService.getDateRange("Today",null);
			Date date5 = new SimpleDateFormat("yyyy-MM-dd").parse(dateRange.get("from_date"));
			String formatString = sessionUtil.getSessionValue("dateFormat");
			if (formatString.isEmpty()) {
				// Default Client PlugIn format
				formatString = "yyyy-MM-dd";
			}
			String dateString2 = new SimpleDateFormat(formatString).format(date5);

			if(array == null)
			{
				//get icici registration details
				CommonTable ct = new CommonTable();
				ct.setOrg_id(org_id);
				ct.setUser_id(user_id);
				ct.setPrimary_key("Icici Registration Details");
				CommonTable ctable = commonTableService.readCommonTable(org_id,user_id,ct);
				if(ctable != null)
				{
					JSONObject datajson = new JSONObject(ctable.getField1());
					String bank_acct_id = datajson.optString("account_no");
					//get from_date from last transaction data
					String from_date = null;
					//get bank transaction details based on bank account id
					Map<String,String> bankMapp = new HashMap<>();
					bankMapp.put("org_id", org_id);
					bankMapp.put("tableId", "114");
					String filterExpression = ("(bank_acct_id=:v_value1)");
					bankMapp.put("filterExpression", filterExpression);
					Map<String, String> bank_filter = new HashMap<String, String>();
					bank_filter.put("filterVal1", bank_acct_id);
					Map<String,String> fieldMap = new HashMap<>();
					fieldMap.put("bank_acct_id",null);
					if(branch_id != null && !branch_id.isEmpty())
					{
						if(branch_id.equals("HeadQuarters"))
						{
							filterExpression = filterExpression +" and ((branch_id=:v_value2) or attribute_not_exists(branch_id))";
						}
						else
						{
							filterExpression = filterExpression +" and (branch_id=:v_value2)";
						}
						bankMapp.put("filterExpression", filterExpression);
						bank_filter.put("filterVal2",branch_id);
						fieldMap.put("branch_id",null);
					}
					TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
					List<Map<String,String>> transList = transObj.readMultiple(bankMapp, bank_filter,fieldMap, "transaction",null);
					// For changing the Date Format
					String format = "dd-MM-yyyy";
					if(transList.size() != 0)
					{
						entity.sortMapList("trans_dt", transList);
						from_date = dateConversion.convertToDisplay(transList.get(transList.size()-1).get("trans_dt"),format);
					}
					if(from_date == null || (from_date != null && from_date.isEmpty()))
					{
						SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
						Date to_dt = new Date();
						Date from_dt= transactionUtil.getDate(to_dt,90);
						from_date = dateFormat.format(from_dt);
					}
					//get the transaction details from icici
					String to_date = new SimpleDateFormat("dd-MM-yyyy").format(date5);
					JSONObject inputjson = new JSONObject();
					inputjson.put("AGGRID",datajson.optString("AGGR_ID"));
					inputjson.put("CORPID",datajson.optString("CORP_ID"));
					inputjson.put("USERID",datajson.optString("USER_ID"));
					inputjson.put("URN",datajson.optString("URN"));
					inputjson.put("ACCOUNTNO",datajson.optString("account_no"));
					inputjson.put("FROMDATE",from_date);
					inputjson.put("TODATE",to_date);

					//String data = "{\"AGGRID\":\"AGGRID\",\"CORPID\":\"CIBNEXT\",\"USERID\":\"CIBTESTING6\",\"URN\":\"URN\",\"ACCOUNTNO\":\"000405001611\",\"FROMDATE\":\"01-01-2016\",\"TODATE\":\"30-12-2016\"}";
					String input = Encryption.encrypt(inputjson.toString());
					//System.out.println("Encrypted : "+input);

					//					String host = "api.icicibank.com:8443";
					//					String url = "https://api.icicibank.com:8443/api/Corporate/CIB/v1/AccountStatement";
					String host = "apibankingone.icicibank.com";
					String url = "https://apibankingone.icicibank.com/api/Corporate/CIB/v1/AccountStatement";
					String apikey = "6a878a6a650b4f928dbe91c7451f65cf";
					String x_forwarded_for = "54.161.37.23";
					if(datajson.optString("uat").equals("true"))
					{
						apikey = "3ad3cee2-c09b-40ad-92ed-d85f994ad2ea";
						url = "https://apigwuat.icicibank.com:8443/api/Corporate/CIB/v1/AccountStatement";
						host = "apigwuat.icicibank.com:8443";
					}

					HttpResponse<String> response = Unirest.post(url)
							.header("Accept", "*/*")
							.header("Content-Type","text/plain")
							.header("apikey",apikey)
							.header("Host",host)
							.header("x-forwarded-for",x_forwarded_for)
							.body(input)
							.asString();
					//System.out.println(response.getBody());
					String message = "Org_id: " +org_id +"  User_id: "+user_id+"  Account Statement output: "+response.getBody();
					LOGGER.log(Level.INFO, message);//write the message in log file in info level
					if(response.getBody() != null)
					{
						//check the response is json object
						JSONObject json = new JSONObject(response.getBody());
						if(!json.optString("encryptedKey").isEmpty())
						{
							//get icici response details 
							String decrypt1=json.optString("encryptedKey");
							String decrypt3 = Encryption.decrypt(decrypt1);
							//System.out.println(decrypt3);
							String decrypt2=json.optString("encryptedData");
							String output = Encryption.decryptByAES(decrypt3, decrypt2);
							//System.out.println(output);
							message = "Org_id: " +org_id +"  User_id: "+user_id+"  Account Statement output: "+output;
							LOGGER.log(Level.INFO, message);//write the message in log file in info level
							JSONObject result = new JSONObject(output);
							if(result.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("MESSAGE"),561);
							}
							if(result.optString("Response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("Message"),561);
							}
							if(result.optString("response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(result.optString("message"),561);
							}
							if(result.optString("RESPONSE").equalsIgnoreCase("SUCCESS"))
							{
								//check the record is json object or json array
								String data = result.optString("Record");
								array = new JSONArray();
								Object jsonin = new JSONTokener(data).nextValue();
								if (jsonin instanceof JSONObject)//if the result is json object then convert the data into json object
								{
									JSONObject object = result.optJSONObject("Record");
									array.put(object);
								}
								else if (jsonin instanceof JSONArray)//if the result is json array then convert the data into json array
								{
									array = result.optJSONArray("Record");
								}
							}
						}
						else
						{
							if(json.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("MESSAGE"),561);
							}
							if(json.optString("Response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("Message"),561);
							}
							if(json.optString("response").equalsIgnoreCase("FAILURE"))
							{
								throw new ZetranGlobalException(json.optString("message"),561);
							}
						}
					}
				}
			}
			//get latest information from banking
			BankAccount bankacc = new BankAccount();
			bankacc.setBank_acct_id(bankMap.optString("bank_acct_id"));
			bankacc.setBranch_id(branch_id);
			List<BankAccount> bankDetails = readBankAccount(bankacc);
			if(bankDetails.size() !=0)
			{
				BankAccount bankObj = bankDetails.get(0);
				if(bankObj.getActive_flg() != null && bankObj.getActive_flg().equals("F"))
				{
					LOGGER.info("Bankfeeds are disabled");
					throw new BankingException("Bankfeeds are disabled", 557,org_id,user_id);
				}
				// Getting Transactions of Bank account

				// Getting the uncategorized existing transactions
				Map<String,String> bankTrnsMap=new HashMap<String,String>();
				bankTrnsMap.put("bank_acct_id",bankMap.getString("bank_acct_id"));
				List<Map<String,String>> unCatExistTransArray =bankTransactionService.readUncategorizedTransactions(bankTrnsMap);

				// Getting the categorized existing transactions
				Map<String,String> catBankTrnsMap=new HashMap<String,String>();
				catBankTrnsMap.put("bank_acct_id",bankMap.getString("bank_acct_id"));
				List<Map<String,String>> catExistTransArray=bankTransactionService.readCategorizedTransactions(catBankTrnsMap);

				//get bank transaction details from icici
				List<Map<String,Object>> bankTransArray = new ArrayList<>();
				if(array != null && array.length() != 0)
				{
					ObjectMapper mapper = new ObjectMapper();
					bankTransArray = mapper.readValue(array.toString(),TypeFactory.defaultInstance().constructCollectionType(List.class,  
							Map.class));
				}
				//System.out.println("bankTransArray:"+bankTransArray);

				DecimalFormat df = new DecimalFormat("####0.00");
				List<Map<String, Object>> bankTransList = new ArrayList<Map<String, Object>>();
				for (int j = 0; j < bankTransArray.size(); j++) {
					// If bank account's accountId is equal to transaction's accountId, then making the entry in bank_trans table
					Map<String, Object> bankTransMap = new HashMap<String, Object>();
					//check the bank trans id used in categorized transaction or not. if it is used then no need to create this
					JSONObject trnsObj=checkTrnsId(unCatExistTransArray,catExistTransArray,bankTransArray.get(j).get("TRANSACTIONID").toString());
					String bank_trans_id=null;
					//this if check the categorize
					if((!trnsObj.getString("bankTransactionId").equals("null") && (!trnsObj.getBoolean("trnsFlg"))))
					{
						continue;
					}
					//these 2 check the uncategorize
					if((!trnsObj.getString("bankTransactionId").equals("null") && trnsObj.getBoolean("trnsFlg")))
					{
						bank_trans_id=trnsObj.getString("bankTransactionId");
					}
					if(trnsObj.getString("bankTransactionId").equals("null") && (!trnsObj.getBoolean("trnsFlg")))
					{
						bank_trans_id=bankTransArray.get(j).get("TRANSACTIONID").toString();
					}
					//create bank transaction entry
					bankTransMap.put("bank_trans_id",bank_trans_id);
					bankTransMap.put("trans_dt", dateConversion.convertToStoreForIndiaBankFeeds(bankTransArray.get(j).get("TXNDATE").toString()));
					bankTransMap.put("trans_txt", bankTransArray.get(j).get("REMARKS"));

					bankTransMap.put("creation_dt", dateConversion.convertToStore(dateString2));
					String amt=bankTransArray.get(j).get("AMOUNT").toString();
					String type = null;
					if(bankTransArray.get(j).get("TYPE") != null)
					{
						type = bankTransArray.get(j).get("TYPE").toString();
					}
					amt = amt.replaceAll(",", "");
					double amount_check = Double.parseDouble(amt);
					// For categorize the Debit and Credit amount
					/** Negative values are goes to CREDIT and 
					 * Positive values are going to DEBIT according to Plaid Documentation **/
					if(type != null && !type.isEmpty())
					{
						bankTransMap.put("deb_cre_ind", type);
						bankTransMap.put("amount_amt", df.format(amount_check));
					}
					else
					{
						if (amount_check<0) {
							bankTransMap.put("deb_cre_ind", "CR");
							bankTransMap.put("amount_amt",df.format(Math.abs(amount_check)));
						} else {
							bankTransMap.put("deb_cre_ind", "DR");
							bankTransMap.put("amount_amt",df.format(amount_check));
						}
					}
					bankTransMap.put("doc_id", bankTransMap.get("bank_trans_id"));
					bankTransMap.put("tableId", "114");
					bankTransMap.put("range_key", "doc_id");
					bankTransMap.put("org_id", org_id);
					bankTransMap.put("branch_id", branch_id);
					bankTransMap.put("bank_acct_id", bankMap.optString("bank_acct_id"));
					bankTransList.add(bankTransMap);
				}

				if(bankTransList.size()!=0)
				{
					TransObj transObj = ApplicationContextUtil.getAppContext().getBean(TransObj.class);
					transObj.createOrUpdate(bankTransList, "transaction");
				}
				/*	else
				{
					LOGGER.info("Doesn't have any transactions for this account");
					throw new BankingException("Doesn't have any transactions for this account", 554,org_id,user_id);
				}*/
				LOGGER.info("Transactions are Successfully Refreshed !");
			}
		}catch(BankingException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch(ZetranGlobalException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * save icici registration information in common table 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public String iciciRegistration(String data)throws Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try {
			//decrypt the input data
			String inputdata = Encryption.decrypt(data);
			String message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciRegistration: "+inputdata;
			LOGGER.log(Level.INFO, message);//write the message in log file in info level
			JSONObject datajson = new JSONObject(inputdata);
			JSONObject inputjson = new JSONObject();
			String corpid="",userid="",aliasid="";
			if(!datajson.optString("corp_id").isEmpty())
			{
				corpid = datajson.optString("corp_id").toUpperCase();
			}
			if(!datajson.optString("user_id").isEmpty())
			{
				userid = datajson.optString("user_id").toUpperCase();
			}
			if(!datajson.optString("alias_id").isEmpty())
			{
				aliasid = datajson.optString("alias_id").toUpperCase();
			}
			inputjson.put("AGGRNAME", "ZETRON");
			inputjson.put("AGGRID", "AGGR0359");
			inputjson.put("CORPID", corpid);
			inputjson.put("USERID", userid);
			inputjson.put("ALIASID", aliasid);
			inputjson.put("URN", org_id);

			//String data ="{\"AGGRNAME\":\"ZETRON\",\"AGGRID\":\"AGGR0359\",\"CORPID\":\"STAUNCH24012012\",\"USERID\":\"PREMALAT\",\"URN\":\"URN\",\"ALIASID\":\"ZETRANPRIMARY\"}";

			String input = Encryption.encrypt(inputjson.toString());
			//System.out.println("Encrypted : "+input);
			//		String apikey = "3ad3cee2-c09b-40ad-92ed-d85f994ad2ea";
			//		String url = "https://apigwuat.icicibank.com:8443/api/Corporate/CIB/v1/Registration";
			//		String host = "apigwuat.icicibank.com:8443";
			//			String host = "api.icicibank.com:8443";
			//			String url = "https://api.icicibank.com:8443/api/Corporate/CIB/v1/Registration";
			String host = "apibankingone.icicibank.com";
			String url = "https://apibankingone.icicibank.com/api/Corporate/CIB/v1/Registration";
			String apikey = "6a878a6a650b4f928dbe91c7451f65cf";
			String x_forwarded_for = "54.161.37.23";

			HttpResponse<String> response = Unirest.post(url)
					.header("Accept", "*/*")
					.header("Content-Type","text/plain")
					.header("apikey",apikey)
					.header("Host",host)
					.header("x-forwarded-for", x_forwarded_for)
					.body(input)
					.asString();
			message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciRegistration output: "+response.getBody();
			LOGGER.log(Level.INFO, message);//write the message in log file in info level
			//System.out.println(response.getBody());
			if(response.getBody() != null)
			{
				//check the response is json object
				Object json11 = new JSONTokener(response.getBody()).nextValue();
				if (json11 instanceof JSONObject)
				{
					JSONObject output = new JSONObject(response.getBody());
					if(output.optString("success").equals("false"))
					{
						throw new ZetranGlobalException(output.optString("message"),561);
					}
				}
				else
				{
					String decrypt1=response.getBody();
					String decrypt3 = Encryption.decrypt(decrypt1);
					message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciRegistration output: "+decrypt3;
					LOGGER.log(Level.INFO, message);//write the message in log file in info level
					//System.out.println(decrypt3);
					JSONObject output = new JSONObject(decrypt3);
					if(output.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
					{
						throw new ZetranGlobalException(output.optString("MESSAGE"),561);
					}
					if(output.optString("Response").equalsIgnoreCase("FAILURE"))
					{
						throw new ZetranGlobalException(output.optString("Message"),561);
					}
					if(output.optString("response").equalsIgnoreCase("FAILURE"))
					{
						throw new ZetranGlobalException(output.optString("message"),561);
					}
					if(output.optString("Response").equalsIgnoreCase("SUCCESS"))
					{
						//saved the corp id userid details in commontable
						List<CommonTable> commonTableobjList = new ArrayList<>();
						CommonTable commonTable = new CommonTable();
						commonTable.setOrg_id(org_id);
						commonTable.setUser_id(user_id);
						commonTable.setPrimary_key("Icici Registration Details");
						output.put("account_no", datajson.optString("account_no"));
						commonTable.setField1(output.toString());
						commonTableobjList.add(commonTable);
						commonTableService.createOrModifyCommonTable(org_id,user_id,commonTableobjList);
						return output.optString("Message");
					}
				}
			}
		}
		catch(ZetranGlobalException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * get the icici registration status  
	 * @return
	 * @throws Exception
	 */
	public String iciciRegistrationStatus()throws Exception
	{
		String org_id=entity.getOrganizationId();
		String user_id=entity.getUserId();
		try {
			//get the icici registration details from common table
			CommonTable ct = new CommonTable();
			ct.setOrg_id(org_id);
			ct.setUser_id(user_id);
			ct.setPrimary_key("Icici Registration Details");
			CommonTable ctable = commonTableService.readCommonTable(org_id,user_id,ct);
			if(ctable != null)
			{
				JSONObject datajson = new JSONObject(ctable.getField1());
				JSONObject inputjson = new JSONObject();
				inputjson.put("AGGRNAME", "ZETRON");
				inputjson.put("AGGRID", "AGGR0359");
				inputjson.put("CORPID", datajson.optString("CORP_ID"));
				inputjson.put("USERID", datajson.optString("USER_ID"));
				inputjson.put("URN", org_id);

				//String data ="{\"AGGRNAME\":\"ZETRON\",\"AGGRID\":\"AGGR0359\",\"CORPID\":\"STAUNCH24012012\",\"USERID\":\"PREMALAT\",\"URN\":\"URN\",\"ALIASID\":\"ZETRANPRIMARY\"}";

				String input = Encryption.encrypt(inputjson.toString());
				//System.out.println("Encrypted : "+input);
				//		String apikey = "3ad3cee2-c09b-40ad-92ed-d85f994ad2ea";
				//		String url = "https://apigwuat.icicibank.com:8443/api/Corporate/CIB/v1/Registration";
				//		String host = "apigwuat.icicibank.com:8443";
				//				String host = "api.icicibank.com:8443";
				//				String url = "https://api.icicibank.com:8443/api/Corporate/CIB/v1/RegistrationStatus";
				String host = "apibankingone.icicibank.com";
				String url = "https://apibankingone.icicibank.com/api/Corporate/CIB/v1/RegistrationStatus";
				String apikey = "6a878a6a650b4f928dbe91c7451f65cf";
				String x_forwarded_for = "54.161.37.23";

				HttpResponse<String> response = Unirest.post(url)
						.header("Accept", "*/*")
						.header("Content-Type","text/plain")
						.header("apikey",apikey)
						.header("Host",host)
						.header("x-forwarded-for", x_forwarded_for)
						.body(input)
						.asString();
				String message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciRegistrationStatus output: "+response.getBody();
				LOGGER.log(Level.INFO, message);//write the message in log file in info level
				//System.out.println(response.getBody());
				if(response.getBody() != null)
				{
					//check the response is json object
					Object json11 = new JSONTokener(response.getBody()).nextValue();
					if (json11 instanceof JSONObject)
					{
						JSONObject output = new JSONObject(response.getBody());
						if(output.optString("success").equals("false"))
						{
							throw new ZetranGlobalException(output.optString("message"),561);
						}
					}
					else
					{
						//get icici response
						String decrypt1=response.getBody();
						String decrypt3 = Encryption.decrypt(decrypt1);
						message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciRegistrationStatus output: "+decrypt3;
						LOGGER.log(Level.INFO, message);//write the message in log file in info level
						//System.out.println(decrypt3);
						if(decrypt3 != null && !decrypt3.isEmpty())
						{
							Object json1 = new JSONTokener(decrypt3).nextValue();
							if (json1 instanceof JSONObject)
							{
								JSONObject output = new JSONObject(decrypt3);
								if(output.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
								{
									throw new ZetranGlobalException(output.optString("MESSAGE"),561);
								}
								if(output.optString("Response").equalsIgnoreCase("FAILURE"))
								{
									throw new ZetranGlobalException(output.optString("Message"),561);
								}
								if(output.optString("response").equalsIgnoreCase("FAILURE"))
								{
									throw new ZetranGlobalException(output.optString("message"),561);
								}
								if(output.optString("Status").equalsIgnoreCase("Registered"))
								{
									addIciciBankFeedAccount();//if it is registered then create icici account
									return "Registration successfull";
								}
								if(output.optString("Status").equalsIgnoreCase("Pending for Self Approval"))
								{
									throw new ZetranGlobalException("Pending for Self Approval",561);
								}
							}
						}
					}
				}
			}
		}
		catch(ZetranGlobalException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}

	/**
	 * delete the icici registration information
	 * @param org_id
	 * @param user_id
	 * @return
	 * @throws Exception
	 */
	public String iciciDeRegistration(String org_id,String user_id)throws Exception
	{
		try {
			//get the icici registration details from common table
			CommonTable ct = new CommonTable();
			ct.setOrg_id(org_id);
			ct.setUser_id(user_id);
			ct.setPrimary_key("Icici Registration Details");
			CommonTable ctable = commonTableService.readCommonTable(org_id,user_id,ct);
			if(ctable != null)
			{
				//call deregistration icici details
				JSONObject datajson = new JSONObject(ctable.getField1());
				JSONObject inputjson = new JSONObject();
				inputjson.put("AGGRNAME", "ZETRON");
				inputjson.put("AGGRID", "AGGR0359");
				inputjson.put("CORPID", datajson.optString("CORP_ID"));
				inputjson.put("USERID", datajson.optString("USER_ID"));
				inputjson.put("URN", org_id);

				//String data ="{\"AGGRNAME\":\"ZETRON\",\"AGGRID\":\"AGGR0359\",\"CORPID\":\"STAUNCH24012012\",\"USERID\":\"PREMALAT\",\"URN\":\"URN\",\"ALIASID\":\"ZETRANPRIMARY\"}";

				String input = Encryption.encrypt(inputjson.toString());
				//System.out.println("Encrypted : "+input);
				//		String apikey = "3ad3cee2-c09b-40ad-92ed-d85f994ad2ea";
				//		String url = "https://apigwuat.icicibank.com:8443/api/Corporate/CIB/v1/Registration";
				//		String host = "apigwuat.icicibank.com:8443";
				//				String host = "api.icicibank.com:8443";
				//				String url = "https://api.icicibank.com:8443/api/Corporate/CIB/v1/Deregistration";
				String host = "apibankingone.icicibank.com";
				String url = "https://apibankingone.icicibank.com/api/Corporate/CIB/v1/Deregistration";
				String apikey = "6a878a6a650b4f928dbe91c7451f65cf";
				String x_forwarded_for = "54.161.37.23";

				HttpResponse<String> response = Unirest.post(url)
						.header("Accept", "*/*")
						.header("Content-Type","text/plain")
						.header("apikey",apikey)
						.header("Host",host)
						.header("x-forwarded-for", x_forwarded_for)
						.body(input)
						.asString();
				String message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciDe-Registration output: "+response.getBody();
				LOGGER.log(Level.INFO, message);//write the message in log file in info level
				//System.out.println(response.getBody());
				if(response.getBody() != null)
				{
					//check the response is json object
					Object json11 = new JSONTokener(response.getBody()).nextValue();
					if (json11 instanceof JSONObject)
					{
						JSONObject output = new JSONObject(response.getBody());
						if(output.optString("success").equals("false"))
						{
							throw new ZetranGlobalException(output.optString("message"),561);
						}
					}
					else
					{
						String decrypt1=response.getBody();
						String decrypt3 = Encryption.decrypt(decrypt1);
						message = "Org_id: " +org_id +"  User_id: "+user_id+"  iciciDe-Registration output: "+decrypt3;
						LOGGER.log(Level.INFO, message);//write the message in log file in info level
						//System.out.println(decrypt3);
						JSONObject output = new JSONObject(decrypt3);
						if(output.optString("RESPONSE").equalsIgnoreCase("FAILURE"))
						{
							throw new ZetranGlobalException(output.optString("MESSAGE"),561);
						}
						if(output.optString("Response").equalsIgnoreCase("FAILURE"))
						{
							throw new ZetranGlobalException(output.optString("Message"),561);
						}
						if(output.optString("response").equalsIgnoreCase("FAILURE"))
						{
							throw new ZetranGlobalException(output.optString("message"),561);
						}
						if(output.optString("Response").equalsIgnoreCase("SUCCESS"))
						{
							return output.optString("Message");
						}
					}
				}
			}
		}
		catch(ZetranGlobalException ex)
		{
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.WARN, message);//LOGGER.log(CustomLogLevel.APPLICATION, message); 
			throw ex;
		}
		catch (Exception ex) {
			String message ="Org_id: " +org_id +"  User_id: "+user_id+"  Exception: "+ex ;
			LOGGER.log(Level.ERROR, message);
			ex.printStackTrace();throw ex;
		}
		return null;
	}
}
