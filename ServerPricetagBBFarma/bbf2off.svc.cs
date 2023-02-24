using System;
using System.IO;
using System.ServiceModel.Web;
using System.Text;
using ServerPricetagBBFarma.Code.Util;
using ServerPricetagBBFarma.Code.Util.Log;
using System.Web;
using System.Security.Cryptography;
using ServerPricetagBBFarma.Core.DbHelper;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ServiceModel.Channels;
using System.ServiceModel;
using System.Data;
using System.Data.SqlClient;
using SDCommon.Barcode;

namespace ServerPricetagBBFarma
{
    public class ServerPricetagBBFarmaImpl : IServerPricetagBBFarma
    {
        #region classi strutture
        public class Articolo
        {
            public String Item;
            public String Description;
        }
        public class FinalProductItem
        {
            public String FINAL_CODE;
            public String FINAL_CODE_INTERNAL;
            public String DISABLED;
            public String TARGET_MARKET;
            public String NHRN_CODE;
            public String FMD_PRODUCT_CODE;
            public String FINAL_DESCRIPTION;
            public String TEMPERATURE;
            public String FRIDGE;
            public String FMD;
            public String FMD_LENGHT;
            public String AIC_CODE;
            public String DIAMETER;
            public String BOX_A;
            public String BOX_B;
            public String BOX_C;
            public String WEIGHT;
            public String ITEM_PER_BOX;
            public String DMC_ROWS;
            public String DMC_COLUMNS;
            public String DMC_DOT_SIZE;
            public String DMC_TYPE;
            public String SERIAL_PPN_PRINT;
            public String FIVE_DIGIT_CODE;
            public String PUBLIC_PRICE;
            public String PRINT_PUBLIC_PRICE;
            public String MASTERPACK_CONFIRMED;
            public String PEI;
            public String PRINT_TYPE;
            public String GROUP_CODE;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class ComponentItem
        {
            public String INITIAL_CODE;
            public String INITIAL_CODE_INTERNAL;
            public String CODNAZ_CODE;
            public String INITIAL_DESCRIPTION;
            public String TYPE;
            public String FMD;
            public String FMD_PRODUCT_CODE;
            public String CURRENT_MAH_CODE;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class ProductCode
        {
            public String INITIAL_CODE;
            public String BATCH;
            public String PPN_PRODUCT_CODE;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class BOMItem
        {
            public String FINAL_CODE;
            public String INITIAL_CODE;
            public String PACK_INITIAL;
            public String PACK_FINAL;
            public String REVERSE_BUNDLE;
            public String REST;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class MahItem
        {
            public String MAH_CODE;
            public String COMPANY_NAME;
            public String ADDRESS;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class ProducerItem
        {
            public String PRODUCER_CODE;
            public String COMPANY_NAME;
            public String ADDRESS;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class LeafLetItem
        {
            public String INITIAL_CODE;
            public String VERSION_CODE;
            public String PRODUCER_CODE;
            public String MAH_CODE;
            public String CMO_CODE;
            public String REVISION;
            public String EXPIRE_DATE;
            public String DISABLED;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class DeliveryNoteItem
        {
            public String DOC_NUMBER;
            public String MAH_CODE;
            public String INITIAL_CODE;
            public String CODNAZ_CODE;
            public String BATCH;
            public String EXPIRE_DATE;
            public String EXPIRE_DATE_INITIAL_CODE;
            public String EXPIRE_DATE_FINAL_CODE;
            public String QUANTITY_INITIAL;
            public String INTRASTAT_PRICE;
            public String FINAL_CODE;
            public String PRODUCER_CODE;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class LotItem
        {
            public String FINAL_CODE;
            public String BATCH;
            public String EXPIRE_DATE;
            public String PRODUCER_CODE;
            public String DISABLED;
            public String MAH_CODE;
            public String ROW_CREATED;
            public String ROW_MODIFIED;
        }
        public class ReworkReq
        {
            public String requestId;
            public String machineNumber;
            public String productCode;
            public String batchNumber;
            public String expireDate;
            public String serialNumber;
            public String reworkOrder;
            public String orderPath;
            public Boolean offline;
            public String username;
        }
        public class CartFreeReq
        {
            public String requestId;
            public String requestDate;
            public CartFreeData data;
        }
        public class CartFreeData
        {
            public String NUM_ORDER;
        }
        public class PickAbortData
        {
            public String NUM_ORDER;
        }
        public class PickData
        {
            public String NUM_ORDER;
            public String FRIDGE;
            public String FINAL_CODE;
            public String CLIENT_CODE;
            public String ORDER_DATE;
            public String BBM_OPERATOR;
            public String ORDER_STATE;
            public List<PickItem> ITEMS;
        }
        public class Box
        {
            public String NUM_BOX;
            public String QTY;
            public String TYPE;
        }
        public class PalletReadyData
        {
            public String ID;
            public String NUM_DELIVERY;
            public String DATE_DELIVERY;
            public String NUM_ORDER_BBM;
            public String NUM_PALLET;
            public String SERIALIZED;
            public String FINAL_CODE;
            public String PRODUCT_CODE_FINAL_CODE;
            public String BATCH_FINAL_CODE;
          //  public String QUANTITY;
            public Box[] BOXES;
        }
        public class PalletReadyReq
        {
            public String requestId;
            public String requestDate;
            public List<PalletReadyData> data;
        }
        public class ProductRefusedReq
        {
            public String requestId;
            public String requestDate;
            public ProductRefusedData data;
        }
        public class ProductRefusedData
        {
            public String NUM_ORDER;
            public String FRIDGE;
            public String FINAL_CODE;
            public String BATCH;
            public String QTY;
        }
        public class ReturnComponentReq
        {
            public String requestId;
            public String requestDate;
            public ReturnComponentData data;
        }
        public class ReturnComponentData
        {
            public String NUM_ORDER;
            public String FINAL_CODE;
            public String FRIDGE;
            public String QTY_FINAL;
            public String[] NUM_CARTS;
            public List<RCBox> BOXES;
            public List<RCItem> ITEMS;
        }
        public class CodedReq
        {
            public String requestId;
            public String requestDate;
            public CodedData data;
        }
        public class CodedData
        {
            public String BATCH_RECORD_BBM;
            public String FINAL_CODE;
            public String BATCH_FINAL_CODE;
            public String INITIAL_CODE;
            public String BATCH_INITIAL_CODE;
            public List<CodedBox> BOXES;
        }
        public class CodedBox
        {
            public String ID;
            public String NUM_BOX;
            public String QTY_BOX;
        }
        public class SerialsReq
        {
            public String requestId;
            public String requestDate;
            public SerialsData data;
        }
        public class SerialsData
        {
            public String BATCH_RECORD_BBM;
            public String FINAL_CODE;
            public String PRODUCT_CODE_FINAL_CODE;
            public String BATCH_FINAL_CODE;
            public String INITIAL_CODE;
            public String PRODUCT_CODE_INITIAL_CODE;
            public String BATCH_INITIAL_CODE;
            public List<Serial> SERIALS;
        }
        public class Serial
        {
            public String ID;
            public String NUM_BOX;
            public String SERIAL_NUMBER_FINAL_CODE;
            public String DATAMATRIX_RAW;
        }
        public class RCBox
        {
            public String NUM_BOX;
            public String USED;
        }
        public class RCItem
        {
            public String INITIAL_CODE;
            public String BATCH;
            public String QTY_RETURNED;
            public String TYPE;
            public String QTY_WASTED;
        }
        public class PickItem
        {
            public String INITIAL_CODE;
            public String BATCH;
            public String QTY;
            public String TYPE;
        }
        public class PickReq
        {
            public String requestId;
            public String requestDate;
            public PickData data;
        }
        public class PickWrong
        {
            public String requestId;
            public String requestDate;
            public PickWrongData data;
        }
        public class PickWrongData
        {
            public String NUM_ORDER;
        }
        public class PickAbort
        {
            public String requestId;
            public String requestDate;
            public PickAbortData data;
        }
        public class StayAliveReq
        {
            public String machineNumber;
        }
        public class ProductionOrder
        {
            public String final_code;
            public Int32 datamatrix_type;
            public Int32 datamatrix_num_of_rows;
            public Int32 datamatrix_num_of_columns;
            public Int32 datamatrix_code_module_size;
            public Int32 datamatrix_minimum_quality;
            public Int32 datamatrix_ppn_label_position;
        }
        public class SerialNumber
        {
            public String string_1_label;
            public String string_1_description;
            public String string_2_label;
            public String string_2_description;
            public String string_3_label;
            public String string_3_description;
            public String string_4_label;
            public String string_4_description;
            public String string_5_label;
            public String string_5_description;
            public String datamatrix_1_dataidentifier;
            public String datamatrix_1_description;
            public String datamatrix_2_dataidentifier;
            public String datamatrix_2_description;
            public String datamatrix_3_dataidentifier;
            public String datamatrix_3_description;
            public String datamatrix_4_dataidentifier;
            public String datamatrix_4_description;
            public String datamatrix_5_dataidentifier;
            public String datamatrix_5_description;
        }
        public class RWResp
        {
            public ProductionOrder productionOrder;
            public List<SerialNumber> serialNumbers;
        }
        public class Layout
        {
            public String finalCode;
            public String serializationUnit;
            public String layout;
            public String lastUpdate;
            public String parameters;
        }
        public class SelResp
        {
            public List<Layout> layouts;
        }
        public class InsertReq
        {
            public String requestId;
            public String machineNumber;
            public String finalCode;
            public String serializationUnit;
            public String layout;
            public String parameters;
            public String username;
        }
        public class UpdateReq
        {
            public String requestId;
            public String machineNumber;
            public String finalCode;
            public String serializationUnit;
            public String layout;
            public String parameters;
            public String username;
        }
        public class DeleteReq
        {
            public String requestId;
            public String machineNumber;
            public String finalCode;
            public String serializationUnit;
            public String username;
        }
        public class DeleteAllReq
        {
            public String requestId;
            public String machineNumber;
            public String username;
        }
        public class SelectReq
        {
            public String requestId;
            public String machineNumber;
            public String finalCode;
            public String serializationUnit;
            public String lastUpdate;
            public String username;
        }
        public class PickConfReq
        {
            public String numOrder;
        }
        public class PickConfResp
        {
            public String wsOk;
            public String wsErrorMessage;
        }
        #endregion
        #region cifratura
        private const string InitVector = "T=A4rAzu94ez-dra";
        private const int KeySize = 256;
        private const int PasswordIterations = 1000; //2;
        private const string SaltValue = "d=?ustAF=UstenAr3B@pRu8=ner5sW&h59_Xe9P2za-eFr2fa&ePHE@ras!a+uc@";
        public static string Decrypt(string encryptedText, string passPhrase)
        {
            byte[] encryptedTextBytes = Convert.FromBase64String(encryptedText);
            byte[] initVectorBytes = Encoding.UTF8.GetBytes(InitVector);
            byte[] passwordBytes = Encoding.UTF8.GetBytes(passPhrase);
            string plainText;
            byte[] saltValueBytes = Encoding.UTF8.GetBytes(SaltValue);

            Rfc2898DeriveBytes password = new Rfc2898DeriveBytes(passwordBytes, saltValueBytes, PasswordIterations);
            byte[] keyBytes = password.GetBytes(KeySize / 8);

            RijndaelManaged rijndaelManaged = new RijndaelManaged { Mode = CipherMode.CBC };

            try
            {
                using (ICryptoTransform decryptor = rijndaelManaged.CreateDecryptor(keyBytes, initVectorBytes))
                {
                    using (MemoryStream memoryStream = new MemoryStream(encryptedTextBytes))
                    {
                        using (CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
                        {
                            //TODO: Need to look into this more. Assuming encrypted text is longer than plain but there is probably a better way
                            byte[] plainTextBytes = new byte[encryptedTextBytes.Length];

                            int decryptedByteCount = cryptoStream.Read(plainTextBytes, 0, plainTextBytes.Length);
                            plainText = Encoding.UTF8.GetString(plainTextBytes, 0, decryptedByteCount);
                        }
                    }
                }
            }
            catch (CryptographicException)
            {
                plainText = string.Empty; // Assume the error is caused by an invalid password
            }

            return plainText;
        }

        public static string Encrypt(string plainText, string passPhrase)
        {
            string encryptedText;
            byte[] initVectorBytes = Encoding.UTF8.GetBytes(InitVector);
            byte[] passwordBytes = Encoding.UTF8.GetBytes(passPhrase);
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            byte[] saltValueBytes = Encoding.UTF8.GetBytes(SaltValue);

            Rfc2898DeriveBytes password = new Rfc2898DeriveBytes(passwordBytes, saltValueBytes, PasswordIterations);
            byte[] keyBytes = password.GetBytes(KeySize / 8);

            RijndaelManaged rijndaelManaged = new RijndaelManaged { Mode = CipherMode.CBC };

            using (ICryptoTransform encryptor = rijndaelManaged.CreateEncryptor(keyBytes, initVectorBytes))
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                    {
                        cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);
                        cryptoStream.FlushFinalBlock();

                        byte[] cipherTextBytes = memoryStream.ToArray();
                        encryptedText = Convert.ToBase64String(cipherTextBytes);
                    }
                }
            }

            return encryptedText;
        }
        #endregion
        #region CheckIP
        private bool CheckIPPT(string addr)
        {
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            string[] IPPT = s.IpServerPriceTag.Split(';');
            // if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1" && addr != s.IpServerMago && addr != s.IpServerPriceTag && addr != s.IpServerPriceTag2 && addr != s.IpServerPriceTag3)
            if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1" && Array.IndexOf(IPPT, addr) == -1 && addr != s.IpServerMago)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        private bool CheckIPRW(string addr)
        {
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            string[] IPRW = s.IpRework.Split(';');
            string[] IPPT = s.IpServerPriceTag.Split(';');
            //if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1" && addr != s.IpRework && addr != s.IpServerPriceTag && addr != s.IpServerMago)
            if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1" && Array.IndexOf(IPRW, addr) == -1 && Array.IndexOf(IPPT, addr) == -1 && addr != s.IpServerMago)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        #endregion
        #region Log
        private void InsertLogRW(int machineid, string req, string resp, string WS, string reworkorder, string requestid, string username, string responseid)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            SqlCommand sCmd = new SqlCommand("insertlogrw");
            sCmd.Connection = dbHelper.cnSQL;
            sCmd.CommandType = CommandType.Text;

            sCmd.CommandText = @"
INSERT INTO TP_bbf2off_RW_Log
           (date_request
           ,id_machine
           ,json_request
           ,json_response
           ,reworkorder
           ,requestid
           ,username
           ,responseid
           ,IP
           ,WS)
     VALUES
           (GETDATE()
           ,@id_machine
           ,@json_request
           ,@json_response
           ,@reworkorder
           ,@requestid
           ,@username
           ,@responseid
           ,@IP
           ,@WS)
";
            sCmd.Parameters.Add("@id_machine", SqlDbType.Int);
            sCmd.Parameters["@id_machine"].Value = machineid;
            sCmd.Parameters.Add("@IP", SqlDbType.VarChar, 15);
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            sCmd.Parameters["@IP"].Value = prop.Address;
            sCmd.Parameters.Add("@json_request", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@json_request"].Value = req;
            sCmd.Parameters.Add("@json_response", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@json_response"].Value = resp;
            sCmd.Parameters.Add("@reworkorder", SqlDbType.VarChar, 100);
            sCmd.Parameters["@reworkorder"].Value = string.IsNullOrEmpty(reworkorder) ? "" : reworkorder;
            sCmd.Parameters.Add("@requestid", SqlDbType.VarChar, 100);
            sCmd.Parameters["@requestid"].Value = string.IsNullOrEmpty(reworkorder) ? "" : requestid;
            sCmd.Parameters.Add("@username", SqlDbType.VarChar, 100);
            sCmd.Parameters["@username"].Value = string.IsNullOrEmpty(reworkorder) ? "" : username;
            sCmd.Parameters.Add("@responseid", SqlDbType.VarChar, 100);
            sCmd.Parameters["@responseid"].Value = string.IsNullOrEmpty(reworkorder) ? "" : responseid;
            sCmd.Parameters.Add("@WS", SqlDbType.VarChar, 50);
            sCmd.Parameters["@WS"].Value = WS;

            var reader = sCmd.ExecuteNonQuery();
        }
        private void InsertLogBBM(string req, string resp, string WS, string requestid, string responseid)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            SqlCommand sCmd = new SqlCommand("insertlogbbm");
            sCmd.Connection = dbHelper.cnSQL;
            sCmd.CommandType = CommandType.Text;
            string code = "";
            code = resp.Substring(resp.IndexOf("status")+9,3);
            string type = "";
            if(code=="200")
            {
                type = "INFO";
            }
            if (code == "400")
            {
                type = "ERR";
            }
            if (code == "500")
            {
                type = "INT";
            }
            sCmd.CommandText = @"
INSERT INTO TP_bbf2off_BBM_Log
           (date_request
           ,json_request
           ,json_response
           ,requestid
           ,responseid
           ,IP
           ,WS
           ,type
           ,code)
     VALUES
           (GETDATE()
           ,@json_request
           ,@json_response
           ,@requestid
           ,@responseid
           ,@IP
           ,@WS
           ,@type
           ,@code)
";
            sCmd.Parameters.Add("@IP", SqlDbType.VarChar, 15);
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            sCmd.Parameters["@IP"].Value = prop.Address;
            sCmd.Parameters.Add("@json_request", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@json_request"].Value = req;
            sCmd.Parameters.Add("@json_response", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@json_response"].Value = resp;
            sCmd.Parameters.Add("@requestid", SqlDbType.VarChar, 100);
            sCmd.Parameters["@requestid"].Value = requestid;
            sCmd.Parameters.Add("@responseid", SqlDbType.VarChar, 100);
            sCmd.Parameters["@responseid"].Value = responseid;
            sCmd.Parameters.Add("@WS", SqlDbType.VarChar, 50);
            sCmd.Parameters["@WS"].Value = WS;
            sCmd.Parameters.Add("@type", SqlDbType.VarChar, 4);
            sCmd.Parameters["@type"].Value = type;
            sCmd.Parameters.Add("@code", SqlDbType.VarChar, 4);
            sCmd.Parameters["@code"].Value = code;
            var reader = sCmd.ExecuteNonQuery();
        }
        private void InsertLog(string sDirezione, string sHash, string sStato)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            SqlCommand sCmd = new SqlCommand("insertlog");
            sCmd.Connection = dbHelper.cnSQL;
            sCmd.CommandType = CommandType.Text;

            sCmd.CommandText = @"
INSERT INTO [dbo].[TP_bbf2off_Log]
           ([Date Time]
           ,[Direction]
           ,[IP]
           ,[Method]
           ,[URI]
           ,[Hash]
           ,[Status])
     VALUES
           (GETDATE()
           ,@Direction
           ,@IP
           ,@Method
           ,@URI
           ,@Hash
           ,@Status)
";
            sCmd.Parameters.Add("@Direction", SqlDbType.VarChar, 3);
            sCmd.Parameters["@Direction"].Value = sDirezione;
            sCmd.Parameters.Add("@IP", SqlDbType.VarChar, 15);
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;

            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];

            string addr = prop.Address;
            sCmd.Parameters["@IP"].Value = prop.Address;// HttpContext.Current.Request.UserHostAddress;
            sCmd.Parameters.Add("@Method", SqlDbType.VarChar, 5);
            sCmd.Parameters["@Method"].Value = "GET";
            sCmd.Parameters.Add("@URI", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@URI"].Value = System.ServiceModel.Web.WebOperationContext.Current.IncomingRequest.UriTemplateMatch.RequestUri.OriginalString;
            sCmd.Parameters.Add("@Hash", SqlDbType.VarChar, 4000);
            sCmd.Parameters["@Hash"].Value = sHash;
            sCmd.Parameters.Add("@Status", SqlDbType.VarChar, 50);
            sCmd.Parameters["@Status"].Value = sStato;

            var reader = sCmd.ExecuteNonQuery();
        }
        #endregion
        #region ReworkMachine
        public System.IO.Stream KeepAliveRW(string MachineId)
        {
            string requestStr = "";
            string wWS = "KeepAlive " + MachineId;
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            //StreamReader param = new StreamReader(jsonStr, Encoding.UTF8);
            //string wReq = param.ReadToEnd();
            //StayAliveReq SArq = JsonConvert.DeserializeObject<StayAliveReq>(wReq);
            try
            {

                if (s.CheckIP == true && CheckIPRW(addr) == false)
                {
                    var responseStr = "{\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(MachineId), "GET", responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (s.CheckIP == true)
                    {
                        if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                        {
                            string[] wMn = addr.Split('.');
                            if (wMn[3] != MachineId)
                            {
                                var responseStr1 = "{\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\"}";
                                byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                                InsertLogRW(Convert.ToInt32(MachineId), "GET", responseStr1, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                                return new MemoryStream(resultBytes1);
                            }
                        }
                    }
                    string responseStr = "{\"status\":\"OK\",\"message\":\"OK\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    InsertLogRW(Convert.ToInt32(MachineId), "GET", responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    return new MemoryStream(resultBytes);
                }
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(MachineId), "GET", responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream StayAlivePOST(StayAliveReq Req)
        {
            string requestStr = "";
            string wWS = "StayAlivePOST";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            //   StreamReader param = new StreamReader(jsonStr, Encoding.UTF8);
            //   string wReq = param.ReadToEnd();
            StayAliveReq SArq = Req;// JsonConvert.DeserializeObject<StayAliveReq>(wReq);
            string wReq = "{\"machineNumber\":\"" + SArq.machineNumber + "\"}";
            try
            {

                if (s.CheckIP == true && CheckIPRW(addr) == false)
                {
                    var responseStr = "{\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);

                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(SArq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (s.CheckIP == true)
                    {
                        if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                        {
                            string[] wMn = addr.Split('.');
                            if (wMn[3] != SArq.machineNumber)
                            {
                                var responseStr1 = "{\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\"}";
                                byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                                InsertLogRW(Convert.ToInt32(SArq.machineNumber), wReq, responseStr1, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                                return new MemoryStream(resultBytes1);
                            }
                        }
                    }
                    string responseStr = "{\"status\":\"OK\",\"message\":\"OK\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    InsertLogRW(Convert.ToInt32(SArq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    return new MemoryStream(resultBytes);
                }
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(SArq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool CheckRespId(string id)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            bool bRet = true;
            try
            {
                SqlCommand sCmd = new SqlCommand("checkrespid");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT *
FROM TP_bbf2off_RW_Log
WHERE responseid = @responseid
";
                sCmd.Parameters.Add("@responseid", SqlDbType.VarChar, 100);
                sCmd.Parameters["@responseid"].Value = id;
                var reader = sCmd.ExecuteReader();

                while (reader.Read())
                {
                    bRet = false;
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            return bRet;
        }
        public System.IO.Stream Rework(ReworkReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string eMsg = "";
            string wWS = "Rework";
            string respId = Guid.NewGuid().ToString();
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            //  StreamReader param = new StreamReader(Req.ToString(), Encoding.UTF8);
            ReworkReq RWrq = Req;//JsonConvert.DeserializeObject<ReworkReq>(wReq);
            if (String.IsNullOrEmpty(RWrq.requestId)) RWrq.requestId = string.Empty;
            string wReq = "{\"requestId\":\"" + RWrq.requestId + "\",\"machineNumber\":\"" + RWrq.machineNumber + "\",\"productCode\":\"" + RWrq.productCode + "\",\"batchNumber\":\"" + RWrq.batchNumber + "\",\"expireDate\":\"" + RWrq.expireDate + "\",\"serialNumber\":\"" + RWrq.serialNumber + "\",\"reworkOrder\":\"" + RWrq.reworkOrder + "\",\"orderPath\":\"" + RWrq.orderPath + "\",\"offline\":\"" + RWrq.offline + "\",\"username\":\"" + RWrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (CheckIPRW(addr) == false)
                {
                    responseStr = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, string.Empty);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != RWrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr1, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, string.Empty);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                bool bErr = false;
                if (RWrq.offline == false)
                {
                    if (bErr == false && String.IsNullOrEmpty(RWrq.productCode))
                    {
                        eMsg = "Product Code obbligatorio";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.batchNumber))
                    {
                        eMsg = "Batch Number obbligatorio";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.expireDate))
                    {
                        eMsg = "Expire Date obbligatoria";
                        bErr = true;
                    }
                    if (bErr == false && RWrq.expireDate.Length != 4 && RWrq.expireDate.Length != 6)
                    {
                        eMsg = "Expire Date di lunghezza non corretta";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.serialNumber))
                    {
                        eMsg = "Serial Number obbligatorio";
                        bErr = true;
                    }
                    if (bErr == false && RWrq.serialNumber.Length != 12)
                    {
                        eMsg = "Serial Number di lunghezza non corretta";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.reworkOrder))
                    {
                        eMsg = "Rework Order obbligatorio";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.orderPath))
                    {
                        eMsg = "OrderPath obbligatorio";
                        bErr = true;
                    }
                    if (bErr == false && String.IsNullOrEmpty(RWrq.username))
                    {
                        eMsg = "Username obbligatorio";
                        bErr = true;
                    }
                    if (bErr == true)
                    {
                        responseStr = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + eMsg + "\",\"data\":\"\"}";
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                        InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, respId);
                    }
                }
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                if (RWrq.offline == false)
                {
                    RWResp retVal = new RWResp();
                    if (bErr == false) retVal = ReworkImpl(RWrq.productCode, RWrq.batchNumber, RWrq.serialNumber, RWrq.expireDate);

                    if (bErr == false && (retVal.productionOrder == null || retVal.serialNumbers == null))
                    {
                        responseStr = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"Serial Number inesistente\",\"data\":\"\"}";
                        resultBytes = Encoding.UTF8.GetBytes(responseStr);
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                        InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, respId);
                        bErr = true;
                    }


                    if (bErr == false)
                    {
                        string wrisp = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(retVal)) + "}";
                        responseStr = wrisp;
                        resultBytes = Encoding.UTF8.GetBytes(responseStr);
                        byte[] hash = new byte[0];
                        using (SHA512 shaM = new SHA512Managed())
                        {
                            hash = shaM.ComputeHash(resultBytes);
                        }
                        WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                        WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                        //if (responseStr != "[]")
                        //{
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                        InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, respId);
                    }
                }
                else
                {
                    if (bErr == false)
                    {
                        RWResp emptyRes = new RWResp();
                        string wrisp = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(emptyRes)) + "}";
                        responseStr = wrisp;
                        resultBytes = Encoding.UTF8.GetBytes(responseStr);
                        byte[] hash = new byte[0];
                        using (SHA512 shaM = new SHA512Managed())
                        {
                            hash = shaM.ComputeHash(resultBytes);
                        }
                        WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                        WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                        InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.reworkOrder, RWrq.requestId, RWrq.username, respId);
                    }
                }

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + RWrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(RWrq.machineNumber), wReq, responseStr, wWS, RWrq.requestId, RWrq.requestId, RWrq.username, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private RWResp ReworkImpl(string sProdCode, string sBatch, string sSerial, string sExpDate)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            //  List<LotItem> artList = new List<LotItem>();
            int lenED = 0;
            if (sExpDate.Length == 4)
            {
                //sExpDate=sExpDate+ DateTime.DaysInMonth(Convert.ToInt32("20"+sExpDate.Substring(0,2)), Convert.ToInt32(sExpDate.Substring(2,2))).ToString();
                lenED = 7;
            }
            else
            {
                lenED = 10;
            }
            RWResp resp = new RWResp();
            try
            {
                SqlCommand sCmd = new SqlCommand("rework");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
select 
pt.final_code
, pt.datamatrix_type
, pt.datamatrix_num_of_rows
, pt.datamatrix_num_of_columns
, pt.datamatrix_code_module_size
, pt.datamatrix_minimum_quality
, pt.datamatrix_ppn_label_position
, pt.string_1_label
, pt.string_1_description
, pt.string_2_label
, pt.string_2_description
, pt.string_3_label
, pt.string_3_description
, pt.string_4_label
, RIGHT(pt.string_4_description,@LEN) as string_4_description
, pt.string_5_label
, pt.string_5_description
, pt.datamatrix_1_dataidentifier
, pt.datamatrix_1_description
, pt.datamatrix_2_dataidentifier
, pt.datamatrix_2_description
, pt.datamatrix_3_dataidentifier
, pt.datamatrix_3_description
, pt.datamatrix_4_dataidentifier
, pt.datamatrix_4_description
, pt.datamatrix_5_dataidentifier
, pt.datamatrix_5_description
from bbf2off_rework as pt
where pt.PRODUCT_CODE_FINAL_CODE=@ProductCode
and pt.BATCH_FINAL_CODE=@Batch 
AND pt.SERIAL_NUMBER_FINAL_CODE=@Serial
AND pt.EXPIRYDATE like @ExpDate+'%'
";
                sCmd.Parameters.Add("@ProductCode", SqlDbType.VarChar, 50);
                sCmd.Parameters["@ProductCode"].Value = sProdCode;
                sCmd.Parameters.Add("@Batch", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Batch"].Value = sBatch;
                sCmd.Parameters.Add("@Serial", SqlDbType.VarChar, 12);
                sCmd.Parameters["@Serial"].Value = sSerial;
                sCmd.Parameters.Add("@ExpDate", SqlDbType.VarChar, 6);
                sCmd.Parameters["@ExpDate"].Value = sExpDate;
                sCmd.Parameters.Add("@LEN", SqlDbType.Int);
                sCmd.Parameters["@LEN"].Value = lenED;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    ProductionOrder po = new ProductionOrder();
                    po.final_code = reader["final_code"].ToString();
                    po.datamatrix_type = Convert.ToInt32(reader["datamatrix_type"]);
                    po.datamatrix_num_of_rows = Convert.ToInt32(reader["datamatrix_num_of_rows"]);
                    po.datamatrix_num_of_columns = Convert.ToInt32(reader["datamatrix_num_of_columns"]);
                    po.datamatrix_code_module_size = Convert.ToInt32(reader["datamatrix_code_module_size"]);
                    po.datamatrix_minimum_quality = Convert.ToInt32(reader["datamatrix_minimum_quality"]);
                    po.datamatrix_ppn_label_position = Convert.ToInt32(reader["datamatrix_ppn_label_position"]);
                    SerialNumber sn = new SerialNumber();
                    sn.string_1_label = reader["string_1_label"].ToString();
                    sn.string_1_description = reader["string_1_description"].ToString();
                    sn.string_2_label = reader["string_2_label"].ToString();
                    sn.string_2_description = reader["string_2_description"].ToString();
                    sn.string_3_label = reader["string_3_label"].ToString();
                    sn.string_3_description = reader["string_3_description"].ToString();
                    sn.string_4_label = reader["string_4_label"].ToString();
                    sn.string_4_description = reader["string_4_description"].ToString();
                    sn.string_5_label = reader["string_5_label"].ToString();
                    sn.string_5_description = reader["string_5_description"].ToString();
                    sn.datamatrix_1_dataidentifier = reader["datamatrix_1_dataidentifier"].ToString();
                    sn.datamatrix_1_description = reader["datamatrix_1_description"].ToString();
                    sn.datamatrix_2_dataidentifier = reader["datamatrix_2_dataidentifier"].ToString();
                    sn.datamatrix_2_description = reader["datamatrix_2_description"].ToString();
                    sn.datamatrix_3_dataidentifier = reader["datamatrix_3_dataidentifier"].ToString();
                    sn.datamatrix_3_description = reader["datamatrix_3_description"].ToString();
                    sn.datamatrix_4_dataidentifier = reader["datamatrix_4_dataidentifier"].ToString();
                    sn.datamatrix_4_description = reader["datamatrix_4_description"].ToString();
                    sn.datamatrix_5_dataidentifier = reader["datamatrix_5_dataidentifier"].ToString();
                    sn.datamatrix_5_description = reader["datamatrix_5_description"].ToString();
                    resp.productionOrder = po;
                    List<SerialNumber> lsn = new List<SerialNumber>();
                    lsn.Add(sn);
                    resp.serialNumbers = lsn;
                    // artList.Add(lot);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return resp;
        }
        public System.IO.Stream Insert(InsertReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string wWS = "Insert";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            string respId = Guid.NewGuid().ToString();
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            InsertReq INrq = Req;
            string wReq = "{\"requestId\":\"" + INrq.requestId + "\",\"machineNumber\":\"" + INrq.machineNumber + "\",\"finalCode\":\"" + INrq.finalCode + "\",\"serializationUnit\":\"" + INrq.serializationUnit + "\",\"layout\":\"" + INrq.layout + "\",\"parameters\":\"" + INrq.parameters + "\",\"username\":\"" + INrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (CheckIPRW(addr) == false)
                {
                    responseStr = "{\"requestId\":\"" + INrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(INrq.machineNumber), wReq, responseStr, wWS, string.Empty, INrq.requestId, INrq.username, respId);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != INrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + INrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(INrq.machineNumber), wReq, responseStr1, wWS, string.Empty, INrq.requestId, INrq.username, respId);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                string wMsgErr = string.Empty;
                bool bErr = false;
                if (String.IsNullOrEmpty(INrq.machineNumber) || String.IsNullOrEmpty(INrq.finalCode) || String.IsNullOrEmpty(INrq.serializationUnit) || String.IsNullOrEmpty(INrq.layout) || String.IsNullOrEmpty(INrq.username))
                {
                    wMsgErr = "Paramentri non corretti";
                    bErr = true;
                }
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                SelResp retVal = new SelResp();
                if (bErr == false) retVal = InsertImpl(INrq.finalCode, INrq.serializationUnit, INrq.layout, INrq.parameters, ref wMsgErr);
                if (retVal.layouts == null || bErr == true)
                {
                    responseStr = "{\"requestId\":\"" + INrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + wMsgErr + "\",\"data\":\"\"}";
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogRW(Convert.ToInt32(INrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                }
                else
                {

                    //  string wrisp = "{\"status\":\"OK\",\"message\":\"OK insert 1 row\"}";
                    string wrisp = "{\"requestId\":\"" + INrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK found 1 row\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(retVal)) + "}";
                    responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogRW(Convert.ToInt32(INrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                }


                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + INrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(INrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private SelResp InsertImpl(string sFinCode, string sSerUnit, string sLayout, string sParam, ref string sMsgErr)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            SelResp resp = new SelResp();
            try
            {
                SqlCommand sCmd = new SqlCommand("insert_check");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
select * 
from
bbf2off_checkfinalcode
where FINAL_CODE=@finalcode
";
                sCmd.Parameters.Add("@finalcode", SqlDbType.VarChar, 50);
                sCmd.Parameters["@finalcode"].Value = sFinCode;
                var reader = sCmd.ExecuteReader();
                if (!reader.HasRows)
                {
                    sMsgErr = "Final code inesistente.";
                    return resp;
                }
                reader.Close();
                SqlCommand chkCmd = new SqlCommand("selectins_check");
                chkCmd.Connection = dbHelper.cnSQL;
                chkCmd.CommandType = CommandType.Text;
                chkCmd.CommandText = @"
SELECT 
*
FROM TP_bbf2off_Layouts
WHERE 1=1
AND final_code=@final_code 
AND serialization_unit=@serialization_unit 
";
                chkCmd.Parameters.Add("@final_code", SqlDbType.VarChar, 50);
                chkCmd.Parameters["@final_code"].Value = sFinCode;
                chkCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                chkCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                var chkreader = chkCmd.ExecuteReader();
                if (chkreader.HasRows)
                {
                    sMsgErr = "Impossibile inserire i dati, Final code e Serialization unit già presenti.";
                    return resp;
                }
                chkreader.Close();
                string query = "INSERT INTO TP_bbf2off_Layouts " +
                "( final_code, serialization_unit, layout, parameters, last_update) " +
                "VALUES (@final_code, @serialization_unit, @layout,@parameters,GETDATE()) ";
                SqlCommand cmd = new SqlCommand(query, dbHelper.cnSQL);
                cmd.Parameters.Add("@final_code", SqlDbType.VarChar, 20).Value = sFinCode;
                cmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 20).Value = sSerUnit;
                cmd.Parameters.Add("@layout", SqlDbType.VarChar, 2000).Value = sLayout;
                cmd.Parameters.Add("@parameters", SqlDbType.VarChar, 200).Value = sParam;
                //    if (dbHelper.cnSQL.State==ConnectionState.Closed) dbHelper.cnSQL.Open();
                int nResp = cmd.ExecuteNonQuery();
                if (nResp == 0)
                {
                    sMsgErr = "Nessun record inserito.";
                    return resp;
                }
                if (nResp != 1)
                {
                    sMsgErr = "Errore inserimento record.";
                    return resp;
                }
                // dbHelper.cnSQL.Close();

                SqlCommand selCmd = new SqlCommand("selectins");
                selCmd.Connection = dbHelper.cnSQL;
                selCmd.CommandType = CommandType.Text;
                selCmd.CommandText = @"
SELECT 
id
, final_code
, serialization_unit
, layout
, parameters
, convert(varchar,last_update,120) as last_update
FROM TP_bbf2off_Layouts
WHERE 1=1
AND final_code=@final_code 
AND serialization_unit=@serialization_unit 
";
                selCmd.Parameters.Add("@final_code", SqlDbType.VarChar, 50);
                selCmd.Parameters["@final_code"].Value = sFinCode;
                selCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                selCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                var selreader = selCmd.ExecuteReader();
                List<Layout> lly = new List<Layout>();
                while (selreader.Read())
                {
                    Layout ly = new Layout();
                    ly.finalCode = selreader["final_code"].ToString();
                    ly.serializationUnit = selreader["serialization_unit"].ToString();
                    ly.layout = selreader["layout"].ToString();
                    ly.lastUpdate = selreader["last_update"].ToString();
                    ly.parameters = selreader["parameters"].ToString();
                    lly.Add(ly);
                }
                resp.layouts = lly;
                return resp;
            }
            catch (Exception exc)
            {
                sMsgErr = exc.Message;
                return resp;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
        }
        public System.IO.Stream Update(UpdateReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string wWS = "Update";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            string respId = Guid.NewGuid().ToString();
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            UpdateReq UPrq = Req;
            string wReq = "{\"requestId\":\"" + UPrq.requestId + "\",\"machineNumber\":\"" + UPrq.machineNumber + "\",\"finalCode\":\"" + UPrq.finalCode + "\",\"serializationUnit\":\"" + UPrq.serializationUnit + "\",\"layout\":\"" + UPrq.layout + "\",\"parameters\":\"" + UPrq.parameters + "\",\"username\":\"" + UPrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (CheckIPRW(addr) == false)
                {
                    responseStr = "{\"requestId\":\"" + UPrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(UPrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != UPrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + UPrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(UPrq.machineNumber), wReq, responseStr1, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                string wMsgErr = string.Empty;
                bool bErr = false;
                if (String.IsNullOrEmpty(UPrq.machineNumber) || String.IsNullOrEmpty(UPrq.finalCode) || String.IsNullOrEmpty(UPrq.serializationUnit) || String.IsNullOrEmpty(UPrq.layout) || String.IsNullOrEmpty(UPrq.username))
                {
                    wMsgErr = "Paramentri non corretti";
                    bErr = true;
                }
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                SelResp retVal = UpdateImpl(UPrq.finalCode, UPrq.serializationUnit, UPrq.layout, UPrq.parameters, ref wMsgErr);
                if (retVal.layouts == null || bErr == true)
                {
                    responseStr = "{\"requestId\":\"" + UPrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + wMsgErr + "\",\"data\":\"\"}";
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogRW(Convert.ToInt32(UPrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                }
                else
                {

                    //   string wrisp = "{\"status\":\"OK\",\"message\":\"OK update 1 row\"}";
                    string wrisp = "{\"requestId\":\"" + UPrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK update 1 row\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(retVal)) + "}";
                    responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogRW(Convert.ToInt32(UPrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                }


                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + UPrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(UPrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private SelResp UpdateImpl(string sFinCode, string sSerUnit, string sLayout, string sParam, ref string sMsgErr)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            SelResp resp = new SelResp();
            try
            {
                SqlCommand sCmd = new SqlCommand("update_check");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
select * 
from
TP_bbf2off_Layouts
where FINAL_CODE=@finalcode AND serialization_unit=@serialization_unit
";
                sCmd.Parameters.Add("@finalcode", SqlDbType.VarChar, 50);
                sCmd.Parameters["@finalcode"].Value = sFinCode;
                sCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                sCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                var reader = sCmd.ExecuteReader();

                DataTable dt = new DataTable();
                dt.Load(reader);
                int numRows = dt.Rows.Count;

                if (numRows == 0)
                {
                    sMsgErr = "Layout non trovato per questi Final code e Serializiation Unit.";
                    return resp;
                }
                if (numRows > 1)
                {
                    sMsgErr = "Layout multipli trovati per questo Final code e Serializiation Unit.";
                    return resp;
                }

                reader.Close();
                string query = "UPDATE TP_bbf2off_Layouts " +
                "SET layout=@layout,parameters=@parameters,last_update=GETDATE() " +
                "WHERE final_code=@final_code AND serialization_unit=@serialization_unit";
                SqlCommand cmd = new SqlCommand(query, dbHelper.cnSQL);
                cmd.Parameters.Add("@final_code", SqlDbType.VarChar, 20).Value = sFinCode;
                cmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 20).Value = sSerUnit;
                cmd.Parameters.Add("@layout", SqlDbType.VarChar, 2000).Value = sLayout;
                cmd.Parameters.Add("@parameters", SqlDbType.VarChar, 200).Value = sParam;
                //    if (dbHelper.cnSQL.State==ConnectionState.Closed) dbHelper.cnSQL.Open();
                int nUpd = cmd.ExecuteNonQuery();
                if (nUpd == 0)
                {
                    sMsgErr = "Nessun record aggiornato.";
                    return resp;
                }

                SqlCommand selCmd = new SqlCommand("selectupd");
                selCmd.Connection = dbHelper.cnSQL;
                selCmd.CommandType = CommandType.Text;
                selCmd.CommandText = @"
SELECT 
id
, final_code
, serialization_unit
, layout
, parameters
, convert(varchar,last_update,120) as last_update
FROM TP_bbf2off_Layouts
WHERE 1=1
AND final_code=@final_code 
AND serialization_unit=@serialization_unit 
";
                selCmd.Parameters.Add("@final_code", SqlDbType.VarChar, 50);
                selCmd.Parameters["@final_code"].Value = sFinCode;
                selCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                selCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                var selreader = selCmd.ExecuteReader();
                List<Layout> lly = new List<Layout>();
                while (selreader.Read())
                {
                    Layout ly = new Layout();
                    ly.finalCode = selreader["final_code"].ToString();
                    ly.serializationUnit = selreader["serialization_unit"].ToString();
                    ly.layout = selreader["layout"].ToString();
                    ly.lastUpdate = selreader["last_update"].ToString();
                    ly.parameters = selreader["parameters"].ToString();
                    lly.Add(ly);
                }
                resp.layouts = lly;
                return resp;
                // dbHelper.cnSQL.Close();
            }
            catch (Exception exc)
            {
                sMsgErr = exc.Message;
                return resp;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

        }
        public System.IO.Stream Delete(DeleteReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string wWS = "Delete";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            string respId = Guid.NewGuid().ToString();
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            DeleteReq DLrq = Req;
            string wReq = "{\"requestId\":\"" + DLrq.requestId + "\",\"machineNumber\":\"" + DLrq.machineNumber + "\",\"finalCode\":\"" + DLrq.finalCode + "\",\"serializationUnit\":\"" + DLrq.serializationUnit + "\",\"username\":\"" + DLrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (CheckIPRW(addr) == false)
                {
                    responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != DLrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr1, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                string wMsgErr = string.Empty;
                bool bErr = false;
                if (String.IsNullOrEmpty(DLrq.machineNumber) || String.IsNullOrEmpty(DLrq.finalCode) || String.IsNullOrEmpty(DLrq.serializationUnit) || String.IsNullOrEmpty(DLrq.username))
                {
                    wMsgErr = "Paramentri non corretti";
                    bErr = true;
                }
                if (bErr == false) bErr = !DeleteImpl(DLrq.finalCode, DLrq.serializationUnit, ref wMsgErr);
                if (bErr == true)
                {
                    responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + wMsgErr + "\",\"data\":\"\"}";
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                }
                else
                {
                    string wrisp = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK delete 1 row\",\"data\":\"\"}";
                    responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\"},\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool DeleteImpl(string sFinCode, string sSerUnit, ref string sMsgErr)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            RWResp resp = new RWResp();
            try
            {
                SqlCommand sCmd = new SqlCommand("delete_check");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
select * 
from
TP_bbf2off_Layouts
where FINAL_CODE=@finalcode AND serialization_unit=@serialization_unit
";
                sCmd.Parameters.Add("@finalcode", SqlDbType.VarChar, 50);
                sCmd.Parameters["@finalcode"].Value = sFinCode;
                sCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                sCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                var reader = sCmd.ExecuteReader();


                DataTable dt = new DataTable();
                dt.Load(reader);
                int numRows = dt.Rows.Count;

                if (numRows == 0)
                {
                    sMsgErr = "Layout non trovato per questi Final code e Serializiation Unit.";
                    return false;
                }
                if (numRows > 1)
                {
                    sMsgErr = "Layout multipli trovati per questo Final code e Serializiation Unit.";
                    return false;
                }

                string query = "DELETE FROM TP_bbf2off_Layouts " +
                               "WHERE final_code=@final_code AND serialization_unit=@serialization_unit";
                SqlCommand cmd = new SqlCommand(query, dbHelper.cnSQL);
                cmd.Parameters.Add("@final_code", SqlDbType.VarChar, 20).Value = sFinCode;
                cmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 20).Value = sSerUnit;
                //    if (dbHelper.cnSQL.State==ConnectionState.Closed) dbHelper.cnSQL.Open();
                cmd.ExecuteNonQuery();

                // dbHelper.cnSQL.Close();
            }
            catch (Exception exc)
            {
                sMsgErr = exc.Message;
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return true;
        }
        public System.IO.Stream DeleteAll(DeleteAllReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string wWS = "DeleteAll";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            string respId = Guid.NewGuid().ToString();
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            DeleteAllReq DLrq = Req;
            string wReq = "{\"requestId\":\"" + DLrq.requestId + "\",\"machineNumber\":\"" + DLrq.machineNumber + "\",\"username\":\"" + DLrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1" && addr != s.IpServerPriceTag)
                {
                    responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != DLrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr1, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                string wMsgErr = string.Empty;
                bool bErr = false;
                if (String.IsNullOrEmpty(DLrq.machineNumber) || String.IsNullOrEmpty(DLrq.username))
                {
                    wMsgErr = "Paramentri non corretti";
                    bErr = true;
                }
                int iVal = 0;
                if (bErr == false) iVal = DeleteAllImpl(ref wMsgErr);
                if (iVal == -1)
                {
                    bErr = true;
                }
                if (bErr == true)
                {
                    responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + wMsgErr + "\",\"data\":\"\"}";
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                }
                else
                {

                    string wrisp = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK delete " + iVal.ToString() + " rows\",\"data\":\"\"}";
                    responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                }


                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + DLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(DLrq.machineNumber), wReq, responseStr, wWS, string.Empty, DLrq.requestId, DLrq.username, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private int DeleteAllImpl(ref string sMsgErr)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            RWResp resp = new RWResp();
            int nRows;
            try
            {
                string query = "DELETE FROM TP_bbf2off_Layouts ";
                SqlCommand cmd = new SqlCommand(query, dbHelper.cnSQL);
                nRows = cmd.ExecuteNonQuery();
            }
            catch (Exception exc)
            {
                sMsgErr = exc.Message;
                return -1;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            return nRows;
        }
        public System.IO.Stream Select(SelectReq Req)
        {
            string requestStr = "";
            string responseStr = "";
            string wWS = "Select";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            string respId = Guid.NewGuid().ToString();
            while (!CheckRespId(respId))
            {
                respId = Guid.NewGuid().ToString();
            }
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            //  StreamReader param = new StreamReader(Req.ToString(), Encoding.UTF8);
            SelectReq SLrq = Req;//JsonConvert.DeserializeObject<ReworkReq>(wReq);
            string wReq = "{\"requestId\":\"" + SLrq.requestId + "\",\"machineNumber\":\"" + SLrq.machineNumber + "\",\"finalCode\":\"" + SLrq.finalCode + "\",\"serializationUnit\":\"" + SLrq.serializationUnit + "\",\"lastUpdate\":\"" + SLrq.lastUpdate + "\",\"username\":\"" + SLrq.username + "\"}";
            if (s.CheckIP == true)
            {
                if (CheckIPRW(addr) == false)
                {
                    responseStr = "{\"requestId\":\"" + SLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    if (addr != "localhost" && addr != "::1" && addr != "127.0.0.1")
                    {
                        string[] wMn = addr.Split('.');
                        if (wMn[3] != SLrq.machineNumber)
                        {
                            var responseStr1 = "{\"requestId\":\"" + SLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                            byte[] resultBytes1 = Encoding.UTF8.GetBytes(responseStr1);
                            WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                            WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                            WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                            InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr1, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                            return new MemoryStream(resultBytes1);
                        }
                    }
                }
            }
            try
            {
                bool bErr = false;
                bool bPar = false;
                if (!String.IsNullOrEmpty(SLrq.finalCode) && !String.IsNullOrEmpty(SLrq.serializationUnit) && String.IsNullOrEmpty(SLrq.lastUpdate))
                {
                    bPar = true;
                }
                if (String.IsNullOrEmpty(SLrq.finalCode) && !String.IsNullOrEmpty(SLrq.serializationUnit) && !String.IsNullOrEmpty(SLrq.lastUpdate))
                {
                    bPar = true;
                }
                if (String.IsNullOrEmpty(SLrq.finalCode) && !String.IsNullOrEmpty(SLrq.serializationUnit) && String.IsNullOrEmpty(SLrq.lastUpdate))
                {
                    bPar = true;
                }
                if (String.IsNullOrEmpty(SLrq.machineNumber) || String.IsNullOrEmpty(SLrq.serializationUnit) || String.IsNullOrEmpty(SLrq.username))
                {
                    bPar = false;
                }
                if (bPar == false)
                {
                    responseStr = "{\"requestId\":\"" + SLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"Paramentri non corretti\",\"data\":\"\"}";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                    bErr = true;
                }
                SelResp retVal = null;
                int nRows = 0;
                if (bErr == false) retVal = SelectImpl(SLrq.finalCode, SLrq.serializationUnit, SLrq.lastUpdate, ref nRows);
                //if (bErr == false && (retVal.layouts == null))
                //{
                //    responseStr = "{\"status\":\"ERROR\",\"message\":\"Nessun dato trovato\"}";
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr, wWS, "");
                //    bErr = true;
                //}
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                if (bErr == false)
                {
                    string wrisp = "{\"requestId\":\"" + SLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"OK\",\"message\":\"OK found " + nRows.ToString() + " rows\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(retVal)) + "}";
                    responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                responseStr = "{\"requestId\":\"" + SLrq.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogRW(Convert.ToInt32(SLrq.machineNumber), wReq, responseStr, wWS, string.Empty, string.Empty, string.Empty, string.Empty);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private SelResp SelectImpl(string sFinCode, string sSerUnit, string dLastUpd, ref int nRighe)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            //  List<LotItem> artList = new List<LotItem>();
            SelResp resp = new SelResp();
            try
            {
                SqlCommand sCmd = new SqlCommand("select");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                string wWhere = string.Empty;
                if (!String.IsNullOrEmpty(sFinCode))
                {
                    wWhere += " AND final_code=@final_code ";
                }
                if (!String.IsNullOrEmpty(sSerUnit))
                {
                    wWhere += " AND serialization_unit=@serialization_unit ";
                }
                if (!String.IsNullOrEmpty(dLastUpd))
                {
                    wWhere += " AND last_update>@last_update ";
                }
                sCmd.CommandText = @"
SELECT 
id
, final_code
, serialization_unit
, layout
, parameters
, convert(varchar,last_update,120) as last_update
FROM TP_bbf2off_Layouts
WHERE 1=1
";
                if (!String.IsNullOrEmpty(sFinCode))
                {
                    sCmd.Parameters.Add("@final_code", SqlDbType.VarChar, 50);
                    sCmd.Parameters["@final_code"].Value = sFinCode;
                }
                if (!String.IsNullOrEmpty(sSerUnit))
                {
                    sCmd.Parameters.Add("@serialization_unit", SqlDbType.VarChar, 50);
                    sCmd.Parameters["@serialization_unit"].Value = sSerUnit;
                }
                if (!String.IsNullOrEmpty(dLastUpd))
                {
                    sCmd.Parameters.Add("@last_update", SqlDbType.DateTime);
                    sCmd.Parameters["@last_update"].Value = Convert.ToDateTime(dLastUpd);
                }
                sCmd.CommandText += wWhere;
                var reader = sCmd.ExecuteReader();
                nRighe = 0;
                List<Layout> lly = new List<Layout>();
                while (reader.Read())
                {
                    Layout ly = new Layout();
                    ly.finalCode = reader["final_code"].ToString();
                    ly.serializationUnit = reader["serialization_unit"].ToString();
                    ly.layout = reader["layout"].ToString();
                    ly.lastUpdate = reader["last_update"].ToString();
                    ly.parameters = reader["parameters"].ToString();
                    lly.Add(ly);
                    nRighe++;
                }
                resp.layouts = lly;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return resp;
        }
        #endregion
        #region HUB
        public System.IO.Stream KeepAlive()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            try
            {
                if (s.CheckIP == true && CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"status\":\"ERROR\",\"message\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
                else
                {
                    string dn = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    // dn = dn.Substring(0, dn.Length - 1);

                    string responseStr = "{\"IP_REQUEST\":\"" + addr + "\",\"DATE_REQUEST\":\"" + dn + @"""}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    return new MemoryStream(resultBytes);
                }
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"status\":\"ERROR\",\"message\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        #region FinalProducts
        public System.IO.Stream FinalProducts(string wDeposito)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<FinalProductItem> retVal = FinalProductsImpl(wDeposito,"");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
           //     if (responseStr != "[]")
              //  {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream FinalProductsInc(string wDeposito, string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<FinalProductItem> retVal = FinalProductsImpl(wDeposito,inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                if (responseStr != "[]")
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                }
                else
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                }

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<FinalProductItem> FinalProductsImpl(string sDeposito, string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            
            List<FinalProductItem> artList = new List<FinalProductItem>();
            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getfinalproducts");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
FINAL_CODE
,FINAL_CODE_INTERNAL
,DISABLED
,TARGET_MARKET
,NHRN_CODE
,FMD_PRODUCT_CODE
,FINAL_DESCRIPTION
,TEMPERATURE
,FRIDGE
,FMD
,FMD_LENGHT
,AIC_CODE
,DIAMETER
,BOX_A
,BOX_B
,BOX_C
,[WEIGHT]
,DMC_ROWS
,DMC_COLUMNS
,DMC_DOT_SIZE
,DMC_TYPE
,SERIAL_PPN_PRINT
,PzPerCT as ITEM_PER_BOX
,FIVE_DIGIT_CODE
,PUBLIC_PRICE
,PRINT_PUBLIC_PRICE
,MASTERPACK_CONFIRMED
,PEI
,PRINT_TYPE
,GROUP_CODE
,ROW_CREATED
,ROW_MODIFIED
FROM bbf2off_finalproducts
WHERE Officina = @Officina

";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " AND DISABLED='NO'";
                }
                else
                { 
                    sCmd.CommandText += " AND ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                sCmd.CommandText += " ORDER BY FINAL_CODE";
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    FinalProductItem fp = new FinalProductItem();
                    fp.FINAL_CODE = reader["FINAL_CODE"].ToString();
                    fp.FINAL_CODE_INTERNAL = reader["FINAL_CODE_INTERNAL"].ToString();
                    fp.DISABLED = reader["DISABLED"].ToString();
                    fp.TARGET_MARKET = reader["TARGET_MARKET"].ToString();
                    fp.FINAL_DESCRIPTION = reader["FINAL_DESCRIPTION"].ToString();
                    fp.NHRN_CODE = reader["NHRN_CODE"].ToString();
                    fp.FMD_PRODUCT_CODE = reader["FMD_PRODUCT_CODE"].ToString();
                    fp.TEMPERATURE = reader["TEMPERATURE"].ToString();
                    fp.FRIDGE = reader["FRIDGE"].ToString();
                    fp.FMD = reader["FMD"].ToString();
                    fp.FMD_LENGHT = reader["FMD_LENGHT"].ToString();
                    fp.AIC_CODE = reader["AIC_CODE"].ToString();
                    fp.DIAMETER = reader["DIAMETER"].ToString();
                    fp.BOX_A = reader["BOX_A"].ToString();
                    fp.BOX_B = reader["BOX_B"].ToString();
                    fp.BOX_C = reader["BOX_C"].ToString();
                    fp.WEIGHT = reader["WEIGHT"].ToString();
                    fp.ITEM_PER_BOX = reader["ITEM_PER_BOX"].ToString();
                    fp.DMC_ROWS = reader["DMC_ROWS"].ToString();
                    fp.DMC_COLUMNS = reader["DMC_COLUMNS"].ToString();
                    fp.DMC_DOT_SIZE = reader["DMC_DOT_SIZE"].ToString();
                    fp.DMC_TYPE = reader["DMC_TYPE"].ToString();
                    fp.SERIAL_PPN_PRINT = reader["SERIAL_PPN_PRINT"].ToString();
                    fp.FIVE_DIGIT_CODE = reader["FIVE_DIGIT_CODE"].ToString();
                    fp.PUBLIC_PRICE = reader["PUBLIC_PRICE"].ToString();
                    fp.PRINT_PUBLIC_PRICE = reader["PRINT_PUBLIC_PRICE"].ToString();
                    fp.MASTERPACK_CONFIRMED = reader["MASTERPACK_CONFIRMED"].ToString();
                    fp.PEI = reader["PEI"].ToString();
                    fp.PRINT_TYPE = reader["PRINT_TYPE"].ToString();
                    fp.GROUP_CODE = reader["GROUP_CODE"].ToString();
                    fp.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    fp.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(fp);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region DeliveryPacking
        public System.IO.Stream DeliveryPackingNoDays(string wDeposito)
        {
            return DeliveryPacking(wDeposito, "7");
        }
        public System.IO.Stream DeliveryPacking(string wDeposito, string wGiorni)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            if (Convert.ToInt32(wGiorni) > 10)
            {
                var responseStr = "{\"error\":true,\"ErrDescr\":\"Numero di giorni non consentito\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request [Numero di giorni non consentito]";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                InsertLog("IN", "", "400 Bad Request [Numero di giorni non consentito]");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
            try
            {
                List<DeliveryNoteItem> retVal = DeliveryPackingImpl(wDeposito, Convert.ToInt32(wGiorni) + 1);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //    if (responseStr != "[]")
                //   {
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<DeliveryNoteItem> DeliveryPackingImpl(string sDeposito, int sGiorni)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            

            List<DeliveryNoteItem> artList = new List<DeliveryNoteItem>();
            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getfinalproducts");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandTimeout = 0;
                sCmd.CommandText = @"
SELECT        
DOC_NUMBER
,INITIAL_CODE
,BATCH
,QUANTITY_INITIAL
,INTRASTAT_PRICE
,FINAL_CODE
,ROW_CREATED
,ROW_MODIFIED
,EXPIRE_DATE
,EXPIRE_DATE_INITIAL_CODE
,EXPIRE_DATE_FINAL_CODE
,CODNAZ_CODE
,MAH_CODE
,PRODUCER_CODE
FROM bbf2off_deliveryrepacking_FN (@giorni,@Officina)
ORDER BY INITIAL_DESCRIPTION,INITIAL_CODE,FINAL_CODE
";
                sCmd.Parameters.Add("@giorni", SqlDbType.Int);
                sCmd.Parameters["@giorni"].Value = sGiorni;
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    DeliveryNoteItem ddt = new DeliveryNoteItem();
                    ddt.DOC_NUMBER = reader["DOC_NUMBER"].ToString();
                    ddt.MAH_CODE = reader["MAH_CODE"].ToString();
                    ddt.INITIAL_CODE = reader["INITIAL_CODE"].ToString();
                    ddt.BATCH = reader["BATCH"].ToString();
                    ddt.QUANTITY_INITIAL = reader["QUANTITY_INITIAL"].ToString();
                    ddt.INTRASTAT_PRICE = reader["INTRASTAT_PRICE"].ToString();
                    ddt.FINAL_CODE = reader["FINAL_CODE"].ToString();
                    ddt.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    ddt.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    ddt.EXPIRE_DATE = reader["EXPIRE_DATE"].ToString();
                    ddt.EXPIRE_DATE_INITIAL_CODE = reader["EXPIRE_DATE_INITIAL_CODE"].ToString();
                    ddt.EXPIRE_DATE_FINAL_CODE = reader["EXPIRE_DATE_FINAL_CODE"].ToString();
                    ddt.CODNAZ_CODE = reader["CODNAZ_CODE"].ToString();
                    ddt.PRODUCER_CODE = reader["PRODUCER_CODE"].ToString();
                    artList.Add(ddt);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region InitialComponents
        public System.IO.Stream InitialComponents(string wDeposito)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ComponentItem> retVal = InitialComponentsImpl(wDeposito,"");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream InitialComponentsInc(string wDeposito, string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ComponentItem> retVal = InitialComponentsImpl(wDeposito,inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<ComponentItem> InitialComponentsImpl(string sDeposito, string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

          
            List<ComponentItem> artList = new List<ComponentItem>();
            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getcomponents");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
INITIAL_CODE
,INITIAL_CODE_INTERNAL
,INITIAL_DESCRIPTION
,[TYPE]
,ROW_CREATED
,ROW_MODIFIED
,CODNAZ_CODE
,FMD
,FMD_PRODUCT_CODE
,CURRENT_MAH_CODE 
,DISABLED
FROM bbf2off_initialcomponents
WHERE Officina = @Officina
";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " AND DISABLED='NO'";
                }
                else
                { 
                    sCmd.CommandText += " AND ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                sCmd.CommandText += " ORDER BY INITIAL_CODE";
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    ComponentItem comp = new ComponentItem();
                    comp.INITIAL_CODE = reader["INITIAL_CODE"].ToString();
                    comp.INITIAL_CODE_INTERNAL = reader["INITIAL_CODE_INTERNAL"].ToString();
                    comp.CODNAZ_CODE = reader["CODNAZ_CODE"].ToString();
                    comp.INITIAL_DESCRIPTION = reader["INITIAL_DESCRIPTION"].ToString();
                    comp.TYPE = reader["TYPE"].ToString();
                    comp.FMD = reader["FMD"].ToString();
                    comp.FMD_PRODUCT_CODE = reader["FMD_PRODUCT_CODE"].ToString();
                    comp.CURRENT_MAH_CODE = reader["CURRENT_MAH_CODE"].ToString();
                    comp.DISABLED = reader["DISABLED"].ToString();
                    comp.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    comp.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(comp);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region FinalProductBatch
        public System.IO.Stream FinalProductBatch()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LotItem> retVal = FinalProductBatchImpl("PRICETAG","");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream FinalProductBatchOff(string Deposito)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LotItem> retVal = FinalProductBatchImpl(Deposito,"");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");

                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");


                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream FinalProductBatchOffInc(string Deposito, string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LotItem> retVal = FinalProductBatchImpl(Deposito,inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");

                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");


                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                return new MemoryStream(resultBytes);
            }
        }
        private List<LotItem> FinalProductBatchImpl(string sDeposito, string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

           

            List<LotItem> artList = new List<LotItem>();

            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getlotslist");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
FINAL_CODE
,BATCH
,EXPIRE_DATE
,PRODUCER_CODE
,MAH_CODE
,DISABLED
,ROW_CREATED
,ROW_MODIFIED
FROM bbf2off_finalproductbatch
WHERE Officina=@Officina";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " AND DISABLED='NO'";
                }
                else
                {
                    sCmd.CommandText += " AND ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";   
                }
                sCmd.CommandText += " ORDER BY FINAL_CODE,BATCH";
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    LotItem lot = new LotItem();
                    lot.FINAL_CODE = reader["FINAL_CODE"].ToString();
                    lot.BATCH = reader["BATCH"].ToString();
                    lot.EXPIRE_DATE = reader["EXPIRE_DATE"].ToString();
                    lot.PRODUCER_CODE = reader["PRODUCER_CODE"].ToString();
                    lot.MAH_CODE = reader["MAH_CODE"].ToString();
                    lot.DISABLED = reader["DISABLED"].ToString();
                    lot.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    lot.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(lot);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region LeafLetVersions
        public System.IO.Stream LeafLetVersions()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LeafLetItem> retVal = LeafLetVersionsImpl("PRICETAG","NO");
                string responseStr = JsonConvert.SerializeObject(retVal, typeof(List<LotItem>), new JsonSerializerSettings());

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream LeafLetVersionsOff(string Deposito)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LeafLetItem> retVal = LeafLetVersionsImpl(Deposito,"");
                string responseStr = JsonConvert.SerializeObject(retVal, typeof(List<LotItem>), new JsonSerializerSettings());

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream LeafLetVersionsOffInc(string Deposito, string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<LeafLetItem> retVal = LeafLetVersionsImpl(Deposito,inc);
                string responseStr = JsonConvert.SerializeObject(retVal, typeof(List<LotItem>), new JsonSerializerSettings());

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<LeafLetItem> LeafLetVersionsImpl(string sDeposito, string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

          

            List<LeafLetItem> artList = new List<LeafLetItem>();
            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getleafletslist");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
INITIAL_CODE
,VERSION_CODE
,PRODUCER_CODE
,MAH_CODE
,CMO_CODE
,REVISION
,EXPIRE_DATE
,DISABLED
,ROW_CREATED
,ROW_MODIFIED
FROM bbf2off_leafletversions
WHERE CMO_CODE in (@Officina,'X')
";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " AND DISABLED='NO'";
                }
                else
                {
                    sCmd.CommandText += " AND ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    LeafLetItem ll = new LeafLetItem();
                    ll.INITIAL_CODE = reader["INITIAL_CODE"].ToString();
                    ll.VERSION_CODE = reader["VERSION_CODE"].ToString();
                    ll.PRODUCER_CODE = reader["PRODUCER_CODE"].ToString();
                    ll.MAH_CODE = reader["MAH_CODE"].ToString();
                    ll.CMO_CODE = reader["CMO_CODE"].ToString();
                    ll.REVISION = reader["REVISION"].ToString();
                    ll.EXPIRE_DATE = reader["EXPIRE_DATE"].ToString();
                    ll.DISABLED = reader["DISABLED"].ToString();
                    ll.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    ll.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(ll);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region BaseLists
        public System.IO.Stream BaseLists(string wDeposito)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<BOMItem> retVal = BaseListsImpl(wDeposito,"");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream BaseListsInc(string wDeposito,string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<BOMItem> retVal = BaseListsImpl(wDeposito,inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                //if (responseStr != "[]")
                //{
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                //}
                //else
                //{
                //    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                //    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                //    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                //}

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<BOMItem> BaseListsImpl(string sDeposito, string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);          

            List<BOMItem> artList = new List<BOMItem>();
            if (sDeposito.ToUpper() != "PRICETAG" && sDeposito.ToUpper() != "BBM") return artList;  
            //forzatura per BBM
            if (sDeposito.ToUpper() == "BBM") sDeposito = "BBOFFIN";
            try
            {
                SqlCommand sCmd = new SqlCommand("getbom");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
FINAL_CODE
,INITIAL_CODE
,PACK_INITIAL
,PACK_FINAL
,REVERSE_BUNDLE 
,REST
,ROW_CREATED
,ROW_MODIFIED
,DISABLED
FROM bbf2off_baselists
WHERE Officina = @Officina";

                if(inc.ToUpper() == "")
                {
                    sCmd.CommandText += " AND DISABLED='NO'";
                }
                else
                { 
                    sCmd.CommandText += " AND ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                sCmd.CommandText += " ORDER BY FINAL_CODE,INITIAL_CODE";
                sCmd.Parameters.Add("@Officina", SqlDbType.VarChar, 50);
                sCmd.Parameters["@Officina"].Value = sDeposito;
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    BOMItem bom = new BOMItem();
                    bom.FINAL_CODE = reader["FINAL_CODE"].ToString();
                    bom.INITIAL_CODE = reader["INITIAL_CODE"].ToString();
                    bom.PACK_INITIAL = reader["PACK_INITIAL"].ToString();
                    bom.PACK_FINAL = reader["PACK_FINAL"].ToString();
                    bom.REVERSE_BUNDLE = reader["REVERSE_BUNDLE"].ToString();
                    bom.REST = reader["REST"].ToString();
                    bom.DISABLED = reader["DISABLED"].ToString();
                    bom.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    bom.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(bom);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region Mah
        public System.IO.Stream Mah()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<MahItem> retVal = MahImpl("");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true,\"ErrDescr\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream MahInc(string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<MahItem> retVal = MahImpl(inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true,\"ErrDescr\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<MahItem> MahImpl(string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            List<MahItem> artList = new List<MahItem>();
            try
            {
                SqlCommand sCmd = new SqlCommand("getmahlist");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
MAH_CODE
, COMPANY_NAME
, ADDRESS
, ROW_CREATED
, ROW_MODIFIED
, DISABLED
FROM bbf2off_mah
";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " WHERE DISABLED='NO'";
                }
                else
                { 
                    sCmd.CommandText += " WHERE ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    MahItem mah = new MahItem();
                    mah.MAH_CODE = reader["MAH_CODE"].ToString();
                    mah.COMPANY_NAME = reader["COMPANY_NAME"].ToString();
                    mah.ADDRESS = reader["ADDRESS"].ToString();
                    mah.DISABLED = reader["DISABLED"].ToString();
                    mah.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    mah.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(mah);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region Producers
        public System.IO.Stream Producers()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ProducerItem> retVal = ProducersImpl("");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true,\"ErrDescr\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream ProducersInc(string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ProducerItem> retVal = ProducersImpl(inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true,\"ErrDescr\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<ProducerItem> ProducersImpl(string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            List<ProducerItem> artList = new List<ProducerItem>();
            try
            {
                SqlCommand sCmd = new SqlCommand("getproducerslist");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT        
PRODUCER_CODE
, COMPANY_NAME
, ADDRESS
, ROW_CREATED
, ROW_MODIFIED
, DISABLED
FROM bbf2off_producers
";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " WHERE DISABLED='NO'";
                }
                else
                {
                    sCmd.CommandText += " WHERE ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    ProducerItem prod = new ProducerItem();
                    prod.PRODUCER_CODE = reader["PRODUCER_CODE"].ToString();
                    prod.COMPANY_NAME = reader["COMPANY_NAME"].ToString();
                    prod.ADDRESS = reader["ADDRESS"].ToString();
                    prod.DISABLED = reader["DISABLED"].ToString();
                    prod.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    prod.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(prod);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #region ProductCodes
        public System.IO.Stream ProductCodes()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ProductCode> retVal = ProductCodesImpl("");
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                if (responseStr != "[]")
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                }
                else
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                }

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public System.IO.Stream ProductCodesInc(string inc)
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    var responseStr = "{\"error\":true,\"ErrDescr\":\"IP richiedente non autorizzato\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLog("IN", "", "403 Forbidden [IP richiedente non autorizzato]");
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                List<ProductCode> retVal = ProductCodesImpl(inc);
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                if (responseStr != "[]")
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                }
                else
                {
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "400 Bad Request");
                }

                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<ProductCode> ProductCodesImpl(string inc)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

            List<ProductCode> artList = new List<ProductCode>();
            try
            {
                SqlCommand sCmd = new SqlCommand("getproductcodess");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;

                sCmd.CommandText = @"
SELECT INITIAL_CODE
      ,BATCH
      ,PPN_PRODUCT_CODE
      ,DISABLED
      ,ROW_CREATED
      ,ROW_MODIFIED
  FROM bbf2off_productcodes
";
                if (inc.ToUpper() == "")
                {
                    sCmd.CommandText += " WHERE DISABLED='NO'";
                }
                else
                { 
                    sCmd.CommandText += " WHERE ROW_MODIFIED>DATEADD(HOUR,-"+inc+",GETDATE())";
                }
                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    ProductCode pc = new ProductCode();
                    pc.INITIAL_CODE = reader["INITIAL_CODE"].ToString();
                    pc.BATCH = reader["BATCH"].ToString();
                    pc.PPN_PRODUCT_CODE = reader["PPN_PRODUCT_CODE"].ToString();
                    pc.DISABLED = reader["DISABLED"].ToString();
                    pc.ROW_CREATED = reader["ROW_CREATED"].ToString();
                    pc.ROW_MODIFIED = reader["ROW_MODIFIED"].ToString();
                    artList.Add(pc);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }
        #endregion
        #endregion
        #region BBM
        public Stream PickingRequest(PickReq req)
        {
            string requestStr = "";
            string wWS = "PickingRequest";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = PickingRequestImpl(req,false, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                  //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

            //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        public Stream PickingRefillRequest(PickReq req)
        {
            string requestStr = "";
            string wWS = "PickingRefillRequest";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = PickingRequestImpl(req, true, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                    //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool PickingRequestImpl(PickReq req, bool refill, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

            if (req== null || req.requestDate == null|| req.requestId == null || req.data==null || req.data.CLIENT_CODE == null 
                || req.data.NUM_ORDER ==null || req.data.FRIDGE == null || req.data.FINAL_CODE == null || req.data.ITEMS == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }

            string sqli = @"
SELECT *
  FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER;
SELECT COUNT(*) as Righe
  FROM TP_BBM_PickReqLines
  WHERE NUM_ORDER=@NUM_ORDER;
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction =ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            if (refill == false)
            {
                if (dti.Rows.Count > 0)
                {
                    Msg = "Numero ordine già acquisito.";
                    return false;
                }
            }
            else
            {
                if (dti.Rows.Count == 0)
                {
                    Msg = "Numero ordine mai acquisito.";
                    return false;
                }
            }
            int nRighe = 0;
            DataTable dtL = dsi.Tables[1];
            if (dtL.Rows.Count > 0)
            {
                DataRow drL= dtL.Rows[0];
                nRighe = Convert.ToInt32(drL["Righe"]);
            }
                string sqlF = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
            SqlDataAdapter daF = new SqlDataAdapter(sqlF, dbHelper.cnSQL);
            SqlCommand cmdF = daF.SelectCommand;
            SqlParameter parF = cmdF.Parameters.Add("@Item", SqlDbType.VarChar, 21);
            parF.Direction = ParameterDirection.Input;
            parF.Value = req.data.FINAL_CODE;
            DataSet dsF = new DataSet("dsF");
            string idTabF = "recF";
            daF.Fill(dsF, idTabF);
            DataTable dtF = dsF.Tables[0];
            if (dtF.Rows.Count == 0)
            {
                Msg = "Articolo finito non trovato.";
                return false;
            }

            bool bOk = true;
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            try
            {
                if (refill == false)
                {
                    SqlCommand sCmdT = new SqlCommand("pickreqimpl");
                    sCmdT.Connection = dbHelper.cnSQL;
                    sCmdT.CommandType = CommandType.Text;
                    sCmdT.Transaction = transaction;
                    sCmdT.CommandText = @"
INSERT INTO TP_BBM_PickReq
           (NUM_ORDER
           , ORDER_DATE
           , BBM_OPERATOR
           , CREATE_DATE
           , ORDER_STATE
           , FRIDGE
           , CLIENT_CODE
           , WMS_CHECKED
           , CANCELLED
)
     VALUES
           (@NUM_ORDER
           , @ORDER_DATE
           , @BBM_OPERATOR
           , @CREATE_DATE
           , @ORDER_STATE
           , @FRIDGE
           , @CLIENT_CODE
           , '0'
           , '0'
           );
";
                    sCmdT.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    sCmdT.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    sCmdT.Parameters.Add("@ORDER_DATE", SqlDbType.DateTime);
                    sCmdT.Parameters["@ORDER_DATE"].Value = req.data.ORDER_DATE == null ? Convert.ToDateTime(DateTime.Now.ToShortDateString()) : Convert.ToDateTime(req.data.ORDER_DATE);
                    sCmdT.Parameters.Add("@BBM_OPERATOR", SqlDbType.VarChar, 50);
                    sCmdT.Parameters["@BBM_OPERATOR"].Value = req.data.BBM_OPERATOR == null ? "" : req.data.BBM_OPERATOR;
                    sCmdT.Parameters.Add("@CREATE_DATE", SqlDbType.DateTime);
                    sCmdT.Parameters["@CREATE_DATE"].Value = DateTime.Now;
                    sCmdT.Parameters.Add("@ORDER_STATE", SqlDbType.VarChar, 12);
                    sCmdT.Parameters["@ORDER_STATE"].Value = req.data.ORDER_STATE == null ? "Creato" : req.data.ORDER_STATE;
                    sCmdT.Parameters.Add("@FRIDGE", SqlDbType.VarChar, 3);
                    sCmdT.Parameters["@FRIDGE"].Value = req.data.FRIDGE.ToUpper() == "NO" ? "0" : "1";
                    sCmdT.Parameters.Add("@CLIENT_CODE", SqlDbType.VarChar, 10);
                    sCmdT.Parameters["@CLIENT_CODE"].Value = req.data.CLIENT_CODE;
                    var readerT = sCmdT.ExecuteNonQuery();
                }
                for (int i = 0; i < req.data.ITEMS.Count; i++)
                {
                    if (req.data.ITEMS[i].BATCH == null  || req.data.ITEMS[i].INITIAL_CODE == null || req.data.ITEMS[i].QTY == null || req.data.ITEMS[i].TYPE == null)
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                    }
                    if (bOk == true)
                    {
                        string sqlG = @"
SELECT InitialQty+ReceivedQty-IssuedQty as Dispo, *
  FROM MA_LotsStoragesQty
  WHERE Item=@Item and Lot=@Lot
  and FiscalYear=YEAR(GETDATE())
  and storage='BBOFFIN'
and @Qty*(select (TP_PackIniziale-TP_Scarto)/TP_PackFinale  from MA_Items where item=@Item)<=InitialQty+ReceivedQty-IssuedQty
";
                        SqlDataAdapter daG = new SqlDataAdapter(sqlG, dbHelper.cnSQL);
                        SqlCommand cmdG = daG.SelectCommand;
                        cmdG.Transaction = transaction;
                        SqlParameter parG = cmdG.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parG.Direction = ParameterDirection.Input;
                        if(req.data.ITEMS[i].TYPE== "COMPONENT")
                        {
                            parG.Value = req.data.ITEMS[i].INITIAL_CODE;
                        }
                        else
                        {
                            parG.Value = req.data.FINAL_CODE;
                        }
                        parG = cmdG.Parameters.Add("@Lot", SqlDbType.VarChar, 16);
                        parG.Direction = ParameterDirection.Input;
                        parG.Value = req.data.ITEMS[i].BATCH;
                        parG = cmdG.Parameters.Add("@Qty", SqlDbType.Float);
                        parG.Direction = ParameterDirection.Input;
                        parG.Value = req.data.ITEMS[i].QTY;
                        DataSet dsG = new DataSet("dsG");
                        string idTabG = "recG";
                        daG.Fill(dsG, idTabG);
                        DataTable dtG = dsG.Tables[0];
                        if (dtG.Rows.Count == 0)
                        {
                            Msg = "Giacenza non disponibile per articolo [" + req.data.ITEMS[i].INITIAL_CODE + "] lotto [" + req.data.ITEMS[i].BATCH + "]";
                            bOk = false;
                        }

                        SqlCommand sCmd = new SqlCommand("pickreqimplLines");
                        sCmd.Connection = dbHelper.cnSQL;
                        sCmd.CommandType = CommandType.Text;
                        sCmd.Transaction = transaction;
                        sCmd.CommandText = @"
INSERT INTO TP_BBM_PickReqLines
           (NUM_ORDER
	        ,FINAL_CODE
	        ,INITIAL_CODE
            ,LINE
	        ,LINE_TYPE
	        ,BATCH
	        ,QTY
            ,CREATE_DATE
)
     VALUES
           (@NUM_ORDER
	       ,@FINAL_CODE
	       ,@INITIAL_CODE
           ,@LINE
	       ,@LINE_TYPE
	       ,@BATCH
	       ,@QTY
           ,@CREATE_DATE
           )
";
                        sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                        sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;                   
                        sCmd.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmd.Parameters.Add("@INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@INITIAL_CODE"].Value = req.data.ITEMS[i].INITIAL_CODE;
                        sCmd.Parameters.Add("@BATCH", SqlDbType.VarChar, 16);
                        sCmd.Parameters["@BATCH"].Value = req.data.ITEMS[i].BATCH;
                        sCmd.Parameters.Add("@LINE", SqlDbType.Int);
                        sCmd.Parameters["@LINE"].Value = nRighe + i + 1;
                        sCmd.Parameters.Add("@QTY", SqlDbType.Float);
                        sCmd.Parameters["@QTY"].Value = req.data.ITEMS[i].QTY;
                        sCmd.Parameters.Add("@LINE_TYPE", SqlDbType.VarChar, 9);
                        sCmd.Parameters["@LINE_TYPE"].Value = req.data.ITEMS[i].TYPE;
                        sCmd.Parameters.Add("@CREATE_DATE", SqlDbType.DateTime);
                        sCmd.Parameters["@CREATE_DATE"].Value = DateTime.Now;
                        var reader = sCmd.ExecuteNonQuery();

                        string sqlA = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
                        SqlDataAdapter daA = new SqlDataAdapter(sqlA, dbHelper.cnSQL);
                        SqlCommand cmdA = daA.SelectCommand;
                        cmdA.Transaction = transaction;
                        SqlParameter parA = cmdA.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parA.Direction = ParameterDirection.Input;
                        parA.Value = req.data.ITEMS[i].INITIAL_CODE;
                        DataSet dsA = new DataSet("dsA");
                        string idTabA = "recA";
                        daA.Fill(dsA, idTabA);
                        DataTable dtA = dsA.Tables[0];

                        if (dtA.Rows.Count == 0)
                        {
                            Msg = "Articolo non trovato.";
                            bOk = false;
                        }
                    }
                }
                if (refill == false)
                {
                    string sql = @"
SELECT *
  FROM TP_BBM_PickReqLines
  WHERE NUM_ORDER=@NUM_ORDER
";
                    SqlDataAdapter da = new SqlDataAdapter(sql, dbHelper.cnSQL);
                    SqlCommand cmd = da.SelectCommand;
                    cmd.Transaction = transaction;
                    SqlParameter par = cmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    par.Direction = ParameterDirection.Input;
                    par.Value = req.data.NUM_ORDER;
                    DataSet ds = new DataSet("ds");
                    string idTab = "rec";
                    da.Fill(ds, idTab);
                    DataTable dt = ds.Tables[0];

                    if (dt.Rows.Count != req.data.ITEMS.Count || bOk == false)
                    {
                        if (bOk == true)
                        {
                            Msg = "Errore inserimento righe.";
                            bOk = false;
                        }
                        SqlCommand sCmdD = new SqlCommand("pickreqdel");
                        sCmdD.Connection = dbHelper.cnSQL;
                        sCmdD.CommandType = CommandType.Text;
                        sCmdD.Transaction = transaction;
                        sCmdD.CommandText = @"
DELETE FROM TP_BBM_PickReqLines
  WHERE NUM_ORDER=@NUM_ORDER;
DELETE FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER;
";
                        sCmdD.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                        sCmdD.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                        var reader = sCmdD.ExecuteNonQuery();
                        return false;
                    }
                }
                if (bOk == true)
                {
                    transaction.Commit();
                    string url = s.WSWApp;
                    using (System.Net.WebClient wc = new System.Net.WebClient())
                    {
                        PickConfReq pcr = new PickConfReq();
                        pcr.numOrder = req.data.NUM_ORDER;
                        string pcrstr = JsonConvert.SerializeObject(pcr);
                        wc.Headers.Add("Content-Type", "application/json; charset=utf-8");
                        //imposto le credenziali per autenticarmi su SharePoint
                        // wc.Credentials = System.Net.CredentialCache.DefaultNetworkCredentials;
                        string result = wc.UploadString(url, pcrstr);
                        PickConfResp resp = JsonConvert.DeserializeObject<PickConfResp>(result);
                        if(resp.wsOk=="false")
                        {
                            Msg = resp.wsErrorMessage;
                            try
                            {
                                SqlCommand sCmdD2 = new SqlCommand("pickreqdel2");
                                sCmdD2.Connection = dbHelper.cnSQL;
                                sCmdD2.CommandType = CommandType.Text;
                                sCmdD2.Transaction = transaction;
                                sCmdD2.CommandText = @"
DELETE FROM TP_BBM_PickReqLines
  WHERE NUM_ORDER=@NUM_ORDER;
DELETE FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER;
";
                                sCmdD2.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                                sCmdD2.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                                var reader2 = sCmdD2.ExecuteNonQuery();
                            }
                            catch (Exception ex2)
                            {
                                Msg = ex2.Message;
                            }
                            return false;
                        }
                    }
                }
                else
                { 
                    transaction.Rollback();
                }
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                try
                {
                    transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    Msg = ex2.Message;
                }
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return true;
        }
        public Stream PickingAbort(PickAbort req)
        {
            string requestStr = "";
            string wWS = "PickingAbort";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = PickingAbortImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");

                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr.Replace("\\", ""), wWS, req.requestId, respId);
                }
                else
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";

                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";// "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500"; //"500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                return new MemoryStream(resultBytes);
            }
        }
        private bool PickingAbortImpl(PickAbort req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            if (req == null || req.data == null || req.requestDate == null || req.requestId == null || req.data.NUM_ORDER == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
SELECT *
  FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            try
            {

                SqlCommand sCmd = new SqlCommand("pickreqimpl");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
UPDATE TP_BBM_PickReq
SET CANCELLED='1'
 WHERE NUM_ORDER=@NUM_ORDER
";
                sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                var reader = sCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return true;
        }
        public Stream PickingWrong(PickWrong req)
        {
            string requestStr = "";
            string wWS = "PickingWrong";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403";// "403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = PickingWrongImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req))+ "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    // var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";

                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";// "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool PickingWrongImpl(PickWrong req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            if (req == null || req.requestDate == null || req.requestId == null || req.data == null || req.data.NUM_ORDER == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            dbHelper.ApriDb(dbConn);
            string sqli = @"
SELECT *
  FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER;
SELECT *
  FROM TP_BBM_PickWrong
  WHERE NUM_ORDER=@NUM_ORDER AND PROCESSED='0';
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            DataTable dtiw = dsi.Tables[1];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            else
            {
                DataRow dri = dti.Rows[0];
                if (Convert.ToString(dri["CANCELLED"]) == "1")
                {
                    Msg = "Ordine già abortito.";
                    return false;
                }
            }
            if (dtiw.Rows.Count > 0)
            {
                Msg = "Numero ordine già in rifacimento.";
                return false;
            }
            try
            {

                SqlCommand sCmd = new SqlCommand("pickwrongimpl");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
INSERT INTO TP_BBM_PickWrong
           (NUM_ORDER
           ,PROCESSED)
     VALUES
           (@NUM_ORDER
           ,'0')
";
                sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                var reader = sCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            
            return true;
        }
        public Stream CartFree(CartFreeReq req)
        {
            string requestStr = "";
            string wWS = "CartFree";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = CartFreeImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");

                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr.Replace("\\", ""), wWS, req.requestId, respId);
                }
                else
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";

                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";// "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500"; //"500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                return new MemoryStream(resultBytes);
            }
        }
        private bool CartFreeImpl(CartFreeReq req, ref string Msg)
        {
            //return true;// per errore in scrittura 
            if (req == null || req.requestDate == null || req.requestId == null || req.data == null || req.data.NUM_ORDER == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
SELECT pr.*,it.BaseUoM
--,pr.QTY*((it.TP_PackIniziale-it.TP_Scarto)/it.TP_PackFinale) as QTYPack
,isnull(bx.QTY,0)*((it.TP_PackIniziale-it.TP_Scarto)/it.TP_PackFinale) as QTYPack
  FROM TP_BBM_PickReqLines as pr
   left outer join MA_Items as it
   on it.Item=case when pr.LINE_TYPE='COMPONENT' then pr.INITIAL_CODE else pr.FINAL_CODE end
 left outer join TP_BBM_PickReqBoxs as bx
   on bx.NUM_ORDER=pr.NUM_ORDER and bx.ITEM_CODE=case when pr.LINE_TYPE='COMPONENT' then pr.INITIAL_CODE else pr.FINAL_CODE end and bx.LOT_CODE=pr.BATCH
  WHERE pr.NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            else
            {

                try
                {
                    SqlCommand sCmd = new SqlCommand("cartfreeimpl");
                    sCmd.Connection = dbHelper.cnSQL;
                    sCmd.CommandType = CommandType.Text;
                    sCmd.CommandText = @"
BEGIN TRAN;
declare @NewId as int
select @NewId= LastId + 1 
from MA_IDNumbers
where CodeType=3801093;

update MA_IDNumbers 
set LastId= @NewId
where CodeType=3801093;

insert into MA_InventoryEntries 
( PreprintedDocNo
, InvRsn
, PostingDate
, DocumentDate
, StoragePhase1
, Specificator1Type
, SpecificatorPhase1
, StoragePhase2
, ReceiptPhase1
, Currency
, EntryId
, AccrualDate
, CustSuppType
, CustSupp
, DocNo
, StubBook
, FromManufacturing
, Notes
 ) 
VALUES
( ''--PreprintedDocNo
, 'CARTFREE'--InvRsn
, CONVERT(DATE,GETDATE())--PostingDate
, CONVERT(DATE,GETDATE())--DocumentDate
, 'BBOFFIN'--StoragePhase1
, 6750211--Specificator1Type
, ''--SpecificatorPhase1
, 'BBOFFPRD'--StoragePhase2
, 0--ReceiptPhase1
, 'EUR'--Currency
, @NewId -- EntryId
, CONVERT(DATE,GETDATE())--AccrualDate
, 6094850--CustSuppType
, ''--CustSupp
, convert(varchar,@NewId)--DocNo
, 'PR'--StubBook
, '0'--FromManufacturing
, ''--Notes
);

";
                    for (int r=0;r<dti.Rows.Count;r++)
                    {
                        DataRow dri = dti.Rows[r];
                        sCmd.CommandText += @"
insert into MA_InventoryEntriesDetail 
(PostingDate
, Item
, UoM
, Qty
, Lot
, EntryId
, Line
, AccrualDate
, EntryTypeForLFBatchEval
, SubId
, BaseUoMQty
, Location
, UnitValue
, Discount1
, Discount2
, DiscountFormula
, DiscountAmount
, LineAmount
, InvRsn
, Description
, Moid
, MONo
, RtgStep
, MOCompLine ) 
VALUES
(  CONVERT(DATE,GETDATE())--PostingDate
, '";
                        if (Convert.ToString(dri["LINE_TYPE"]) == "COMPONENT")
                        {
                            sCmd.CommandText += Convert.ToString(dri["INITIAL_CODE"]);
                        }
                        else
                        {
                            sCmd.CommandText += Convert.ToString(dri["FINAL_CODE"]);
                        }
                        sCmd.CommandText += "','"+Convert.ToString(dri["BaseUoM"]) + "'," + Convert.ToString(dri["QTYPack"])+",'"+ Convert.ToString(dri["BATCH"]);
                        sCmd.CommandText += @"'
, @NewId -- EntryId
, ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @",  CONVERT(DATE,GETDATE())--AccrualDate
, 12255235--det.EntryTypeForLFBatchEval
,  ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @"
,  ";
                        sCmd.CommandText += Convert.ToString(dri["QTYPack"]);
                        sCmd.CommandText += @",''-- Location
, 0--UnitValue
, 0--Discount1
, 0--Discount2
, ''--DiscountFormula
, 0--DiscountAmount
, 0--LineAmount
, 'CARTFREE'--InvRsn
, ''--Description
, 0--Moid
, ''--MONo
, 0--RtgStep
, 0--MOCompLine
);

";

                       
                    }
                    sCmd.CommandText += @"
insert into  Exs_WriteMovMag 
(DocType, DocId, Status) 
VALUES 
('MOVMAG',@NewId,'INS');
COMMIT TRAN;
";
                 //   sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                //    sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Msg = ex.Message;
                    if (Msg.Contains("trigger")) Msg = "Errore creando il Movimento di Magazzino.";
                    return false;
                }
                finally
                {
                    if (dbHelper != null)
                    {
                        dbHelper.ChiudiDb();
                        dbHelper = null;
                    }
                }
            }
            return true;
        }
        public Stream PalletReady(PalletReadyReq req)
        {
            string requestStr = "";
            string wWS = "PalletReady";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = PalletReadyImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");

                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr.Replace("\\", ""), wWS, req.requestId, respId);
                }
                else
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";

                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";// "400 Bad Request";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500"; //"500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                return new MemoryStream(resultBytes);
            }
        }
        private bool PalletReadyImpl(PalletReadyReq req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            if (req == null || req.data == null || req.requestId == null || req.requestDate == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            int MaxID = 0;
            try
            {
                string sql = @"
                    SELECT isnull(MAX(IDIMPORTAZIONE),0) AS IDMax
                    FROM  TP_BBM_PalletReady  WITH (NOLOCK)";
                SqlDataAdapter da = new SqlDataAdapter(sql, dbHelper.cnSQL);
                SqlCommand cmd = da.SelectCommand;
                cmd.CommandTimeout = 600;
                DataSet ds = new DataSet("ds");
                string idTab = "rec";
                da.Fill(ds, idTab);
                DataTable dt = ds.Tables[0];
                if (dt.Rows.Count > 0)
                {
                    DataRow dr = dt.Rows[0];
                    MaxID = Convert.ToInt32(dr["IDMax"]) + 1;
                }
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                return false;
            }
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            foreach (PalletReadyData data in req.data)
            {
                if (data.BATCH_FINAL_CODE == null || data.BOXES == null
               || data.DATE_DELIVERY == null || data.FINAL_CODE == null || data.ID == null || data.NUM_DELIVERY == null || data.NUM_ORDER_BBM == null
               || data.NUM_PALLET == null || data.PRODUCT_CODE_FINAL_CODE == null || data.SERIALIZED == null)
                {
                    Msg = "Struttura non corretta.";
                    transaction.Rollback();
                    return false;
                }

              
                PalletReadyData d = data;
                if (d.BOXES != null && d.BOXES.Length > 0)
                {
                    foreach (Box b in d.BOXES)
                    {
                        if (b.NUM_BOX == null || b.QTY == null || b.TYPE == null)
                        {
                            Msg = "Struttura non corretta.";
                            transaction.Rollback();
                            return false;
                        }
                        try
                        {
                            SqlCommand sCmd = new SqlCommand("insertpallet");
                            sCmd.Connection = dbHelper.cnSQL;
                            sCmd.CommandTimeout = 600;
                            sCmd.CommandType = CommandType.Text;
                            sCmd.Transaction = transaction;
                            sCmd.CommandText = @"
INSERT INTO TP_BBM_PalletReady
           (ID
           ,NUM_DELIVERY
           ,DATE_DELIVERY
           ,NUM_ORDER_BBM
           ,NUM_PALLET
           ,TYPE
           ,SERIALIZED
           ,FINAL_CODE
           ,PRODUCT_CODE_FINAL_CODE
           ,BATCH_FINAL_CODE
           ,QUANTITY
           ,PT_BOX_CODE
           ,PT_BOX_QUANTITY
           ,IDIMPORTAZIONE)
     VALUES
           (@ID
           ,@NUM_DELIVERY
           ,@DATE_DELIVERY
           ,@NUM_ORDER_BBM
           ,@NUM_PALLET
           ,@TYPE
           ,@SERIALIZED
           ,@FINAL_CODE
           ,@PRODUCT_CODE_FINAL_CODE
           ,@BATCH_FINAL_CODE
           ,@QUANTITY
           ,@PT_BOX_CODE
           ,@PT_BOX_QUANTITY
           ,@IDIMPORTAZIONE)
    ";
                            sCmd.Parameters.Add("@ID", SqlDbType.Int);
                            sCmd.Parameters["@ID"].Value = d.ID;
                            sCmd.Parameters.Add("@NUM_DELIVERY", SqlDbType.VarChar, 15);
                            sCmd.Parameters["@NUM_DELIVERY"].Value = d.NUM_DELIVERY;
                            sCmd.Parameters.Add("@DATE_DELIVERY", SqlDbType.DateTime);
                            sCmd.Parameters["@DATE_DELIVERY"].Value = d.DATE_DELIVERY;
                            sCmd.Parameters.Add("@TYPE", SqlDbType.VarChar, 10);
                            sCmd.Parameters["@TYPE"].Value = b.TYPE;
                            sCmd.Parameters.Add("@SERIALIZED", SqlDbType.VarChar, 10);
                            sCmd.Parameters["@SERIALIZED"].Value = d.SERIALIZED;
                            sCmd.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                            sCmd.Parameters["@FINAL_CODE"].Value = d.FINAL_CODE;
                            sCmd.Parameters.Add("@PRODUCT_CODE_FINAL_CODE", SqlDbType.VarChar, 30);
                            sCmd.Parameters["@PRODUCT_CODE_FINAL_CODE"].Value = d.PRODUCT_CODE_FINAL_CODE;
                            sCmd.Parameters.Add("@BATCH_FINAL_CODE", SqlDbType.VarChar, 16);
                            sCmd.Parameters["@BATCH_FINAL_CODE"].Value = d.BATCH_FINAL_CODE;
                            sCmd.Parameters.Add("@QUANTITY", SqlDbType.Float);
                            sCmd.Parameters["@QUANTITY"].Value = 0;// d.QUANTITY;
                            sCmd.Parameters.Add("@PT_BOX_CODE", SqlDbType.VarChar, 15);
                            sCmd.Parameters["@PT_BOX_CODE"].Value = b.NUM_BOX;
                            sCmd.Parameters.Add("@PT_BOX_QUANTITY", SqlDbType.Float);
                            sCmd.Parameters["@PT_BOX_QUANTITY"].Value = b.QTY;
                            sCmd.Parameters.Add("@NUM_PALLET", SqlDbType.VarChar, 15);
                            sCmd.Parameters["@NUM_PALLET"].Value = d.NUM_PALLET;
                            sCmd.Parameters.Add("@NUM_ORDER_BBM", SqlDbType.VarChar, 20);
                            sCmd.Parameters["@NUM_ORDER_BBM"].Value = d.NUM_ORDER_BBM;
                            sCmd.Parameters.Add("@IDIMPORTAZIONE", SqlDbType.Int);
                            sCmd.Parameters["@IDIMPORTAZIONE"].Value = MaxID;
                            var readerT = sCmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Msg = ex.Message;
                            transaction.Rollback();
                            return false;
                        }

                    }
                }
                else
                {

                    try
                    {
                        SqlCommand sCmd = new SqlCommand("insertpallet");
                        sCmd.Connection = dbHelper.cnSQL;
                        sCmd.CommandTimeout = 600;
                        sCmd.CommandType = CommandType.Text;
                        sCmd.Transaction = transaction;
                        sCmd.CommandText = @"
INSERT INTO TP_BBM_PalletReady
           (ID
           ,NUM_DELIVERY
           ,DATE_DELIVERY
           ,NUM_ORDER_BBM
           ,NUM_PALLET
           ,TYPE
           ,SERIALIZED
           ,FINAL_CODE
           ,PRODUCT_CODE_FINAL_CODE
           ,BATCH_FINAL_CODE
           ,QUANTITY
           ,PT_BOX_CODE
           ,PT_BOX_QUANTITY
           ,IDIMPORTAZIONE)
     VALUES
           (@ID
           ,@NUM_DELIVERY
           ,@DATE_DELIVERY
           ,@NUM_ORDER_BBM
           ,@NUM_PALLET
           ,@TYPE
           ,@SERIALIZED
           ,@FINAL_CODE
           ,@PRODUCT_CODE_FINAL_CODE
           ,@BATCH_FINAL_CODE
           ,@QUANTITY
           ,@PT_BOX_CODE
           ,@PT_BOX_QUANTITY
           ,@IDIMPORTAZIONE)
    ";
                        sCmd.Parameters.Add("@ID", SqlDbType.Int);
                        sCmd.Parameters["@ID"].Value = d.ID;
                        sCmd.Parameters.Add("@NUM_DELIVERY", SqlDbType.VarChar, 15);
                        sCmd.Parameters["@NUM_DELIVERY"].Value = d.NUM_DELIVERY;
                        sCmd.Parameters.Add("@DATE_DELIVERY", SqlDbType.DateTime);
                        sCmd.Parameters["@DATE_DELIVERY"].Value = d.DATE_DELIVERY;
                        sCmd.Parameters.Add("@TYPE", SqlDbType.VarChar, 10);
                        sCmd.Parameters["@TYPE"].Value = "";
                        sCmd.Parameters.Add("@SERIALIZED", SqlDbType.VarChar, 10);
                        sCmd.Parameters["@SERIALIZED"].Value = d.SERIALIZED;
                        sCmd.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@FINAL_CODE"].Value = d.FINAL_CODE;
                        sCmd.Parameters.Add("@PRODUCT_CODE_FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@PRODUCT_CODE_FINAL_CODE"].Value = d.PRODUCT_CODE_FINAL_CODE;
                        sCmd.Parameters.Add("@BATCH_FINAL_CODE", SqlDbType.VarChar, 16);
                        sCmd.Parameters["@BATCH_FINAL_CODE"].Value = d.BATCH_FINAL_CODE;
                        sCmd.Parameters.Add("@QUANTITY", SqlDbType.Float);
                        sCmd.Parameters["@QUANTITY"].Value = 0;// d.QUANTITY;
                        sCmd.Parameters.Add("@PT_BOX_CODE", SqlDbType.VarChar, 15);
                        sCmd.Parameters["@PT_BOX_CODE"].Value = "";
                        sCmd.Parameters.Add("@PT_BOX_QUANTITY", SqlDbType.Float);
                        sCmd.Parameters["@PT_BOX_QUANTITY"].Value = 0;
                        sCmd.Parameters.Add("@NUM_PALLET", SqlDbType.VarChar, 15);
                        sCmd.Parameters["@NUM_PALLET"].Value = d.NUM_PALLET;
                        sCmd.Parameters.Add("@NUM_ORDER_BBM", SqlDbType.VarChar, 20);
                        sCmd.Parameters["@NUM_ORDER_BBM"].Value = d.NUM_ORDER_BBM;
                        sCmd.Parameters.Add("@IDIMPORTAZIONE", SqlDbType.Int);
                        sCmd.Parameters["@IDIMPORTAZIONE"].Value = MaxID;
                        var readerT = sCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Msg = ex.Message;
                        transaction.Rollback();
                        return false;
                    }

                }
                try
                {
                    SqlCommand sCmdDb = new SqlCommand("updqtypalletready");
                    sCmdDb.Connection = dbHelper.cnSQL;
                    sCmdDb.CommandType = CommandType.Text;
                    sCmdDb.Transaction = transaction;
                    sCmdDb.CommandText = @"
update p
set QUANTITY=g.Tot
from TP_BBM_PalletReady p
left outer join (
select IDIMPORTAZIONE,ID,SUM(PT_BOX_QUANTITY) as Tot  
from TP_BBM_PalletReady
where IDIMPORTAZIONE=@IdImportazione
group by IDIMPORTAZIONE,ID) g
on g.ID=p.id and p.IDIMPORTAZIONE=g.IDIMPORTAZIONE
where p.IDIMPORTAZIONE=@IdImportazione
";
                    sCmdDb.Parameters.Add("@IdImportazione", SqlDbType.Int);
                    sCmdDb.Parameters["@IdImportazione"].Value = MaxID;
                    var readerDb = sCmdDb.ExecuteNonQuery();
                }
                catch (Exception exu)
                {
                    Msg = exu.Message;
                    transaction.Rollback();
                    return false;
                }
            }
            transaction.Commit();
            return true;
        }
        public Stream ProductRefused(ProductRefusedReq req)
        {
            string requestStr = "";
            string wWS = "ProductRefused";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = ProductRefusedImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                    //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool ProductRefusedImpl(ProductRefusedReq req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            if (req == null || req.requestDate == null || req.requestId == null || req.data == null || req.data.NUM_ORDER == null || req.data.BATCH == null || req.data.FINAL_CODE == null || req.data.FRIDGE == null || req.data.QTY == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

            string sqli = @"
SELECT *
  FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine mai acquisito.";
                return false;
            }

            string sqlr = @"
SELECT *
  FROM TP_BBM_ProductRefused
  WHERE NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dar = new SqlDataAdapter(sqlr, dbHelper.cnSQL);
            SqlCommand cmdr = dar.SelectCommand;
            SqlParameter parr = cmdr.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            parr.Direction = ParameterDirection.Input;
            parr.Value = req.data.NUM_ORDER;
            DataSet dsr = new DataSet("dsi");
            string idTabr = "reci";
            dar.Fill(dsr, idTabr);
            DataTable dtr = dsr.Tables[0];
            if (dtr.Rows.Count > 0)
            {
                Msg = "Prodotti rifiutati già presenti per questo ordine.";
                return false;
            }

            bool bOk = true;
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            try
            {
                SqlCommand sCmdb = new SqlCommand("productrefusedimpl");
                sCmdb.Connection = dbHelper.cnSQL;
                sCmdb.CommandType = CommandType.Text;
                sCmdb.Transaction = transaction;
                sCmdb.CommandText = @"
INSERT INTO TP_BBM_ProductRefused
           (NUM_ORDER
           ,FRIDGE
           ,FINAL_CODE
           ,BATCH
           ,QTY)
     VALUES
           (@NUM_ORDER
           ,@FRIDGE
           ,@FINAL_CODE
           ,@BATCH
           ,@QTY)
";
                sCmdb.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                sCmdb.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                sCmdb.Parameters.Add("@FRIDGE", SqlDbType.VarChar, 3);
                sCmdb.Parameters["@FRIDGE"].Value = req.data.FRIDGE;
                sCmdb.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                sCmdb.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                sCmdb.Parameters.Add("@BATCH", SqlDbType.VarChar, 16);
                sCmdb.Parameters["@BATCH"].Value = req.data.BATCH;
                sCmdb.Parameters.Add("@QTY", SqlDbType.Float);
                sCmdb.Parameters["@QTY"].Value = req.data.QTY;
                var readerb = sCmdb.ExecuteNonQuery();

                ProductRefusedMM(req.data.NUM_ORDER, ref Msg);
                transaction.Commit();
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                transaction.Rollback();
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            return true;
        }
        private bool ProductRefusedMM(string NrOrd, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
 select 
 rc.FINAL_CODE
 ,rc.BATCH
 ,rc.QTY
 ,it.BaseUoM from 
 TP_BBM_ProductRefused as rc WITH (NOLOCK)
    left outer join MA_Items as it WITH (NOLOCK)
   on it.Item=rc.FINAL_CODE
  WHERE rc.NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = NrOrd;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
         //   SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
              //  transaction.Rollback();
                return false;
            }
            else
            {                
                try
                {
                    SqlCommand sCmd = new SqlCommand("ProductRefusedMM");
                    sCmd.Connection = dbHelper.cnSQL;
                    sCmd.CommandType = CommandType.Text;
                   // sCmd.Transaction = transaction;
                    sCmd.CommandText = @"
BEGIN TRAN;
declare @NewId as int
select @NewId= LastId + 1 
from MA_IDNumbers
where CodeType=3801093;

update MA_IDNumbers 
set LastId= @NewId
where CodeType=3801093;

insert into MA_InventoryEntries 
( PreprintedDocNo
, InvRsn
, PostingDate
, DocumentDate
, StoragePhase1
, Specificator1Type
, SpecificatorPhase1
, StoragePhase2
, ReceiptPhase1
, Currency
, EntryId
, AccrualDate
, CustSuppType
, CustSupp
, DocNo
, StubBook
, FromManufacturing
, Notes
 ) 
VALUES
( ''--PreprintedDocNo
, 'REFPRD'--InvRsn
, CONVERT(DATE,GETDATE())--PostingDate
, CONVERT(DATE,GETDATE())--DocumentDate
, 'BBOFFPRD'--StoragePhase1
, 6750211--Specificator1Type
, ''--SpecificatorPhase1
, 'SMALTIME'--StoragePhase2
, 0--ReceiptPhase1
, 'EUR'--Currency
, @NewId -- EntryId
, CONVERT(DATE,GETDATE())--AccrualDate
, 6094850--CustSuppType
, ''--CustSupp
, convert(varchar,@NewId)--DocNo
, 'PR'--StubBook
, '0'--FromManufacturing
, ''--Notes
);

";
                    for (int r = 0; r < dti.Rows.Count; r++)
                    {
                        DataRow dri = dti.Rows[r];
                        sCmd.CommandText += @"
insert into MA_InventoryEntriesDetail 
(PostingDate
, Item
, UoM
, Qty
, Lot
, EntryId
, Line
, AccrualDate
, EntryTypeForLFBatchEval
, SubId
, BaseUoMQty
, Location
, UnitValue
, Discount1
, Discount2
, DiscountFormula
, DiscountAmount
, LineAmount
, InvRsn
, Description
, Moid
, MONo
, RtgStep
, MOCompLine ) 
VALUES
(  CONVERT(DATE,GETDATE())--PostingDate
, '";

                        sCmd.CommandText += Convert.ToString(dri["FINAL_CODE"]);
                        sCmd.CommandText += "','" + Convert.ToString(dri["BaseUoM"]) + "'," + Convert.ToString(dri["QTY"]) + ",'" + Convert.ToString(dri["BATCH"]);
                        sCmd.CommandText += @"'
, @NewId -- EntryId
, ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @",  CONVERT(DATE,GETDATE())--AccrualDate
, 12255235--det.EntryTypeForLFBatchEval
,  ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @"
,  ";
                        sCmd.CommandText += Convert.ToString(dri["QTY"]);
                        sCmd.CommandText += @",''-- Location
, 0--UnitValue
, 0--Discount1
, 0--Discount2
, ''--DiscountFormula
, 0--DiscountAmount
, 0--LineAmount
, 'RCFIN'--InvRsn
, ''--Description
, 0--Moid
, ''--MONo
, 0--RtgStep
, 0--MOCompLine
);

";
                    }
                    sCmd.CommandText += @"
insert into  Exs_WriteMovMag 
(DocType, DocId, Status) 
VALUES 
('MOVMAG',@NewId,'INS');
COMMIT TRAN;
";
                    //   sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    //    sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Msg = ex.Message;
                  //  transaction.Rollback();
                    return false;
                }
                finally
                {
                    if (dbHelper != null)
                    {
                        dbHelper.ChiudiDb();
                        dbHelper = null;
                    }
                }
            }
          //  transaction.Commit();
            return true;
        }
        public Stream ReturnComponent(ReturnComponentReq req)
        {
            string requestStr = "";
            string wWS = "ReturnComponent";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = ReturnComponentImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                    //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool ReturnComponentImpl(ReturnComponentReq req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");

            if (req == null || req.requestDate == null || req.requestId == null || req.data == null || req.data.NUM_ORDER == null || req.data.QTY_FINAL == null || req.data.FINAL_CODE == null || req.data.FRIDGE == null || req.data.ITEMS == null || req.data.NUM_CARTS == null || req.data.BOXES == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

            string sqli = @"
SELECT *
  FROM TP_BBM_PickReq
  WHERE NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = req.data.NUM_ORDER;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine mai acquisito.";
                return false;
            }      
            bool bOk = true;
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            try
            {

                for (int i = 0; i < req.data.BOXES.Count; i++)
                {
                    if (req.data.BOXES[i].NUM_BOX == null || req.data.BOXES[i].USED == null )
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                    }
                    if (bOk == true)
                    {
                        SqlCommand sCmdb = new SqlCommand("retcompboximpl");
                        sCmdb.Connection = dbHelper.cnSQL;
                        sCmdb.CommandType = CommandType.Text;
                        sCmdb.Transaction = transaction;
                        sCmdb.CommandText = @"
INSERT INTO TP_BBM_ReturnCompBoxes
           (NUM_ORDER
           ,FINAL_CODE
           ,QTY_FINAL
           ,NUM_CARTS
           ,NUM_BOX
           ,USED
           ,PROCESSED)
     VALUES
           (@NUM_ORDER
           ,@FINAL_CODE
           ,@QTY_FINAL
           ,@NUM_CARTS
           ,@NUM_BOX
           ,@USED
           ,'0')
";
                        sCmdb.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                        sCmdb.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                        sCmdb.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmdb.Parameters.Add("@QTY_FINAL", SqlDbType.Float);
                        sCmdb.Parameters["@QTY_FINAL"].Value = req.data.QTY_FINAL;
                        sCmdb.Parameters.Add("@NUM_CARTS", SqlDbType.VarChar, 4000);
                        sCmdb.Parameters["@NUM_CARTS"].Value = string.Join(",",req.data.NUM_CARTS);
                        sCmdb.Parameters.Add("@NUM_BOX", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@NUM_BOX"].Value = req.data.BOXES[i].NUM_BOX;
                        sCmdb.Parameters.Add("@USED", SqlDbType.VarChar, 3);
                        sCmdb.Parameters["@USED"].Value = req.data.BOXES[i].USED;
                        var readerb = sCmdb.ExecuteNonQuery();                       

                        string sqlBX = @"
SELECT *
  FROM TP_Box WITH (NOLOCK)
  WHERE BoxCode=@BoxCode
";
                        SqlDataAdapter daBX = new SqlDataAdapter(sqlBX, dbHelper.cnSQL);
                        SqlCommand cmdBX = daBX.SelectCommand;
                        cmdBX.Transaction = transaction;
                        SqlParameter parBX = cmdBX.Parameters.Add("@BoxCode", SqlDbType.VarChar, 30);
                        parBX.Direction = ParameterDirection.Input;
                        parBX.Value = req.data.BOXES[i].NUM_BOX;
                        DataSet dsBX = new DataSet("dsBX");
                        string idTabBX = "recBX";
                        daBX.Fill(dsBX, idTabBX);
                        DataTable dtBX = dsBX.Tables[0];
                        if (dtBX.Rows.Count == 0)
                        {
                            Msg = "Scatola "+ req.data.BOXES[i].NUM_BOX+" non trovata.";
                            transaction.Rollback();
                            bOk = false;
                        }
                    }
                }

                string sqlck = @"
SELECT * 
  FROM TP_BBM_ReturnCompBoxes WITH (NOLOCK)
  WHERE NUM_ORDER=@NUM_ORDER
";
                SqlDataAdapter dack = new SqlDataAdapter(sqlck, dbHelper.cnSQL);
                SqlCommand cmdck = dack.SelectCommand;
                cmdck.Transaction = transaction;
                SqlParameter parck = cmdck.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                parck.Direction = ParameterDirection.Input;
                parck.Value = req.data.NUM_ORDER;
                DataSet dsck = new DataSet("dsck");
                string idTabck = "recck";
                dack.Fill(dsck, idTabck);
                DataTable dtck = dsck.Tables[0];

                if (dtck.Rows.Count != req.data.BOXES.Count || bOk == false)
                {
                    if (bOk == true) Msg = "Errore inserimento righe scatole.";
                    SqlCommand sCmdDb = new SqlCommand("retcompboxdel");
                    sCmdDb.Connection = dbHelper.cnSQL;
                    sCmdDb.CommandType = CommandType.Text;
                    sCmdDb.Transaction = transaction;
                    sCmdDb.CommandText = @"
DELETE FROM TP_BBM_ReturnCompBoxes
  WHERE NUM_ORDER=@NUM_ORDER
";
                    sCmdDb.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    sCmdDb.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var readerDb = sCmdDb.ExecuteNonQuery();
                    return false;
                }


                for (int i = 0; i < req.data.ITEMS.Count; i++)
                {
                    if (req.data.ITEMS[i].BATCH == null ||  req.data.ITEMS[i].INITIAL_CODE == null || req.data.ITEMS[i].QTY_RETURNED == null || req.data.ITEMS[i].TYPE == null || req.data.ITEMS[i].QTY_WASTED == null)
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                        transaction.Rollback();
                        return false;
                    }
                    if (bOk == true)
                    {
                        SqlCommand sCmd = new SqlCommand("retcompitemimpl");
                        sCmd.Connection = dbHelper.cnSQL;
                        sCmd.CommandType = CommandType.Text;
                        sCmd.Transaction = transaction;
                        sCmd.CommandText = @"
INSERT INTO TP_BBM_ReturnCompItems
           (NUM_ORDER
           ,FINAL_CODE
           ,QTY_FINAL
           ,NUM_CARTS
           ,INITIAL_CODE
           ,BATCH
           ,QTY_RETURNED
           ,TYPE
           ,FRIDGE
           ,QTY_WASTED
           ,PROCESSED)
     VALUES
           (@NUM_ORDER
           ,@FINAL_CODE
           ,@QTY_FINAL
           ,@NUM_CARTS
           ,@INITIAL_CODE
           ,@BATCH
           ,@QTY_RETURNED
           ,@TYPE
           ,@FRIDGE
           ,@QTY_WASTED
           ,'0')
";
                        sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                        sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                        sCmd.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmd.Parameters.Add("@QTY_FINAL", SqlDbType.Float);
                        sCmd.Parameters["@QTY_FINAL"].Value = req.data.QTY_FINAL;
                        sCmd.Parameters.Add("@NUM_CARTS", SqlDbType.VarChar, 4000);
                        sCmd.Parameters["@NUM_CARTS"].Value = string.Join(",", req.data.NUM_CARTS);
                        sCmd.Parameters.Add("@INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@INITIAL_CODE"].Value = req.data.ITEMS[i].INITIAL_CODE;
                        sCmd.Parameters.Add("@BATCH", SqlDbType.VarChar, 16);
                        sCmd.Parameters["@BATCH"].Value = req.data.ITEMS[i].BATCH;
                        sCmd.Parameters.Add("@QTY_RETURNED", SqlDbType.Float);
                        sCmd.Parameters["@QTY_RETURNED"].Value = req.data.ITEMS[i].QTY_RETURNED;
                        sCmd.Parameters.Add("@TYPE", SqlDbType.VarChar, 9);
                        sCmd.Parameters["@TYPE"].Value = req.data.ITEMS[i].TYPE;
                        sCmd.Parameters.Add("@FRIDGE", SqlDbType.VarChar, 9);
                        sCmd.Parameters["@FRIDGE"].Value = req.data.FRIDGE;
                        sCmd.Parameters.Add("@QTY_WASTED", SqlDbType.Float);
                        sCmd.Parameters["@QTY_WASTED"].Value = req.data.ITEMS[i].QTY_WASTED;
                        var reader = sCmd.ExecuteNonQuery();
                        string sqlF = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
                        SqlDataAdapter daF = new SqlDataAdapter(sqlF, dbHelper.cnSQL);
                        SqlCommand cmdF = daF.SelectCommand;
                        cmdF.Transaction = transaction;
                        SqlParameter parF = cmdF.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parF.Direction = ParameterDirection.Input;
                        parF.Value = req.data.FINAL_CODE;
                        DataSet dsF = new DataSet("dsF");
                        string idTabF = "recF";
                        daF.Fill(dsF, idTabF);
                        DataTable dtF = dsF.Tables[0];

                        if (dtF.Rows.Count == 0)
                        {
                            Msg = "Articolo non trovato.";
                            bOk = false;
                            transaction.Rollback();
                            return false;
                        }

                        string sqlA = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
                        SqlDataAdapter daA = new SqlDataAdapter(sqlA, dbHelper.cnSQL);
                        SqlCommand cmdA = daA.SelectCommand;
                        cmdA.Transaction = transaction;
                        SqlParameter parA = cmdA.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parA.Direction = ParameterDirection.Input;
                        parA.Value = req.data.ITEMS[i].INITIAL_CODE;
                        DataSet dsA = new DataSet("dsA");
                        string idTabA = "recA";
                        daA.Fill(dsA, idTabA);
                        DataTable dtA = dsA.Tables[0];

                        if (dtA.Rows.Count == 0)
                        {
                            Msg = "Componente non trovato.";
                            bOk = false;
                            transaction.Rollback();
                            return false;
                        }
                    }
                }

                string sql = @"
SELECT *
  FROM TP_BBM_ReturnCompItems
  WHERE NUM_ORDER=@NUM_ORDER
";
                SqlDataAdapter da = new SqlDataAdapter(sql, dbHelper.cnSQL);
                SqlCommand cmd = da.SelectCommand;
                cmd.Transaction = transaction;
                SqlParameter par = cmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                par.Direction = ParameterDirection.Input;
                par.Value = req.data.NUM_ORDER;
                DataSet ds = new DataSet("ds");
                string idTab = "rec";
                da.Fill(ds, idTab);
                DataTable dt = ds.Tables[0];

                if (dt.Rows.Count != req.data.ITEMS.Count || bOk == false)
                {
                    if (bOk == true) Msg = "Errore inserimento righe articoli.";
                    SqlCommand sCmdD = new SqlCommand("retcompitemdel");
                    sCmdD.Connection = dbHelper.cnSQL;
                    sCmdD.CommandType = CommandType.Text;
                    sCmdD.Transaction = transaction;
                    sCmdD.CommandText = @"
DELETE FROM TP_BBM_ReturnCompItems
  WHERE NUM_ORDER=@NUM_ORDER
";
                    sCmdD.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    sCmdD.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmdD.ExecuteNonQuery();
                    transaction.Rollback();
                    return false;
                }
                ReturnCompMMret(req.data.NUM_ORDER, ref Msg);
                ReturnCompMMprod(req.data.NUM_ORDER, ref Msg);
                ReturnCompMMcons(req.data.NUM_ORDER, ref Msg);
                transaction.Commit();
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                transaction.Rollback();
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            return true;
        }
        private bool ReturnCompMMret(string NrOrd, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
  SELECT pr.*,pr.QTY_RETURNED as QTY,it.BaseUoM,pr.QTY_RETURNED*(it.TP_PackIniziale/it.TP_PackFinale) as QTYPack
  FROM TP_BBM_ReturnCompItems as pr WITH (NOLOCK)
   left outer join MA_Items as it WITH (NOLOCK)
   on it.Item=case when pr.TYPE='COMPONENT' then pr.INITIAL_CODE else pr.FINAL_CODE end
  WHERE pr.NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER",SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = NrOrd;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            else
            {

                try
                {
                    SqlCommand sCmd = new SqlCommand("ReturnCompMMret");
                    sCmd.Connection = dbHelper.cnSQL;
                    sCmd.CommandType = CommandType.Text;
                    sCmd.CommandText = @"
BEGIN TRAN;
declare @NewId as int
select @NewId= LastId + 1 
from MA_IDNumbers
where CodeType=3801093;

update MA_IDNumbers 
set LastId= @NewId
where CodeType=3801093;

insert into MA_InventoryEntries 
( PreprintedDocNo
, InvRsn
, PostingDate
, DocumentDate
, StoragePhase1
, Specificator1Type
, SpecificatorPhase1
, StoragePhase2
, ReceiptPhase1
, Currency
, EntryId
, AccrualDate
, CustSuppType
, CustSupp
, DocNo
, StubBook
, FromManufacturing
, Notes
 ) 
VALUES
( ''--PreprintedDocNo
, 'RETCOMP'--InvRsn
, CONVERT(DATE,GETDATE())--PostingDate
, CONVERT(DATE,GETDATE())--DocumentDate
, 'BBOFFPRD'--StoragePhase1
, 6750211--Specificator1Type
, ''--SpecificatorPhase1
, 'BBOFFIN'--StoragePhase2
, 0--ReceiptPhase1
, 'EUR'--Currency
, @NewId -- EntryId
, CONVERT(DATE,GETDATE())--AccrualDate
, 6094850--CustSuppType
, ''--CustSupp
, convert(varchar,@NewId)--DocNo
, 'PR'--StubBook
, '0'--FromManufacturing
, ''--Notes
);

";
                    for (int r = 0; r < dti.Rows.Count; r++)
                    {
                        DataRow dri = dti.Rows[r];
                        sCmd.CommandText += @"
insert into MA_InventoryEntriesDetail 
(PostingDate
, Item
, UoM
, Qty
, Lot
, EntryId
, Line
, AccrualDate
, EntryTypeForLFBatchEval
, SubId
, BaseUoMQty
, Location
, UnitValue
, Discount1
, Discount2
, DiscountFormula
, DiscountAmount
, LineAmount
, InvRsn
, Description
, Moid
, MONo
, RtgStep
, MOCompLine ) 
VALUES
(  CONVERT(DATE,GETDATE())--PostingDate
, '";
                        if (Convert.ToString(dri["TYPE"]) == "COMPONENT")
                        {
                            sCmd.CommandText += Convert.ToString(dri["INITIAL_CODE"]);
                        }
                        else
                        {
                            sCmd.CommandText += Convert.ToString(dri["FINAL_CODE"]);
                        }
                        sCmd.CommandText += "','" + Convert.ToString(dri["BaseUoM"]) + "'," + Convert.ToString(dri["QTYPack"]) + ",'" + Convert.ToString(dri["BATCH"]);
                        sCmd.CommandText += @"'
, @NewId -- EntryId
, ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @",  CONVERT(DATE,GETDATE())--AccrualDate
, 12255235--det.EntryTypeForLFBatchEval
,  ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @"
,  ";
                        sCmd.CommandText += Convert.ToString(dri["QTYPack"]);
                        sCmd.CommandText += @",''-- Location
, 0--UnitValue
, 0--Discount1
, 0--Discount2
, ''--DiscountFormula
, 0--DiscountAmount
, 0--LineAmount
, 'RETCOMP'--InvRsn
, ''--Description
, 0--Moid
, ''--MONo
, 0--RtgStep
, 0--MOCompLine
);

";
                    }
                    sCmd.CommandText += @"
insert into  Exs_WriteMovMag 
(DocType, DocId, Status) 
VALUES 
('MOVMAG',@NewId,'INS');
COMMIT TRAN;
";
                    //   sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    //    sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Msg = ex.Message;
                    return false;
                }
                finally
                {
                    if (dbHelper != null)
                    {
                        dbHelper.ChiudiDb();
                        dbHelper = null;
                    }
                }
            }
            return true;
        }
        private bool ReturnCompMMprod(string NrOrd, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
 select 
 rc.FINAL_CODE
 ,pr.BATCH
 ,rc.QTY_FINAL
 ,it.BaseUoM from 
 TP_BBM_ReturnCompItems as rc WITH (NOLOCK)
 left outer join (SELECT * FROM TP_BBM_PickReqLines WITH (NOLOCK) where LINE_TYPE='PRODUCT')  as pr
 on pr.NUM_ORDER=rc.NUM_ORDER
    left outer join MA_Items as it
   on it.Item=rc.FINAL_CODE
  WHERE rc.NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = NrOrd;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            else
            {

                try
                {
                    SqlCommand sCmd = new SqlCommand("ReturnCompMMprod");
                    sCmd.Connection = dbHelper.cnSQL;
                    sCmd.CommandType = CommandType.Text;
                    sCmd.CommandText = @"
BEGIN TRAN;
declare @NewId as int
select @NewId= LastId + 1 
from MA_IDNumbers
where CodeType=3801093;

update MA_IDNumbers 
set LastId= @NewId
where CodeType=3801093;

insert into MA_InventoryEntries 
( PreprintedDocNo
, InvRsn
, PostingDate
, DocumentDate
, StoragePhase1
, Specificator1Type
, SpecificatorPhase1
, StoragePhase2
, ReceiptPhase1
, Currency
, EntryId
, AccrualDate
, CustSuppType
, CustSupp
, DocNo
, StubBook
, FromManufacturing
, Notes
 ) 
VALUES
( ''--PreprintedDocNo
, 'RCFIN'--InvRsn
, CONVERT(DATE,GETDATE())--PostingDate
, CONVERT(DATE,GETDATE())--DocumentDate
, 'BBOFFPRD'--StoragePhase1
, 6750211--Specificator1Type
, ''--SpecificatorPhase1
, 'BBOFFQ'--StoragePhase2
, 0--ReceiptPhase1
, 'EUR'--Currency
, @NewId -- EntryId
, CONVERT(DATE,GETDATE())--AccrualDate
, 6094850--CustSuppType
, ''--CustSupp
, convert(varchar,@NewId)--DocNo
, 'PR'--StubBook
, '0'--FromManufacturing
, ''--Notes
);

";
                    for (int r = 0; r < dti.Rows.Count; r++)
                    {
                        DataRow dri = dti.Rows[r];
                        sCmd.CommandText += @"
insert into MA_InventoryEntriesDetail 
(PostingDate
, Item
, UoM
, Qty
, Lot
, EntryId
, Line
, AccrualDate
, EntryTypeForLFBatchEval
, SubId
, BaseUoMQty
, Location
, UnitValue
, Discount1
, Discount2
, DiscountFormula
, DiscountAmount
, LineAmount
, InvRsn
, Description
, Moid
, MONo
, RtgStep
, MOCompLine ) 
VALUES
(  CONVERT(DATE,GETDATE())--PostingDate
, '";

                        sCmd.CommandText += Convert.ToString(dri["FINAL_CODE"]);
                        sCmd.CommandText += "','" + Convert.ToString(dri["BaseUoM"]) + "'," + Convert.ToString(dri["QTY_FINAL"]) + ",'" + Convert.ToString(dri["BATCH"]);
                        sCmd.CommandText += @"'
, @NewId -- EntryId
, ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @",  CONVERT(DATE,GETDATE())--AccrualDate
, 12255235--det.EntryTypeForLFBatchEval
,  ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @"
,  ";
                        sCmd.CommandText += Convert.ToString(dri["QTY_FINAL"]);
                        sCmd.CommandText += @",''-- Location
, 0--UnitValue
, 0--Discount1
, 0--Discount2
, ''--DiscountFormula
, 0--DiscountAmount
, 0--LineAmount
, 'RCFIN'--InvRsn
, ''--Description
, 0--Moid
, ''--MONo
, 0--RtgStep
, 0--MOCompLine
);

";
                    }
                    sCmd.CommandText += @"
insert into  Exs_WriteMovMag 
(DocType, DocId, Status) 
VALUES 
('MOVMAG',@NewId,'INS');
COMMIT TRAN;
";
                    //   sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    //    sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Msg = ex.Message;
                    return false;
                }
                finally
                {
                    if (dbHelper != null)
                    {
                        dbHelper.ChiudiDb();
                        dbHelper = null;
                    }
                }
            }
            return true;
        }
        private bool ReturnCompMMcons(string NrOrd, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            string sqli = @"
 select 
  case when pr.LINE_TYPE='COMPONENT' then pr.INITIAL_CODE else pr.FINAL_CODE end ITEM
 ,pr.BATCH
 ,pr.QTY*((it.TP_PackIniziale-it.TP_Scarto)/it.TP_PackFinale)+isnull(rc.QTY_WASTED,0) as QTY
 ,it.BaseUoM from 
 TP_BBM_ReturnCompItems as rc WITH (NOLOCK)
 right outer join TP_BBM_PickReqLines  as pr WITH (NOLOCK)
 on pr.NUM_ORDER=rc.NUM_ORDER and pr.INITIAL_CODE=rc.INITIAL_CODE
    left outer join MA_Items as it WITH (NOLOCK)
   on it.Item=case when pr.LINE_TYPE='COMPONENT' then pr.INITIAL_CODE else pr.FINAL_CODE end
   where  pr.QTY*(it.TP_PackIniziale/it.TP_PackFinale)+isnull(rc.QTY_WASTED,0)>0
  AND pr.NUM_ORDER=@NUM_ORDER
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            SqlParameter pari = cmdi.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
            pari.Direction = ParameterDirection.Input;
            pari.Value = NrOrd;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];

            if (dti.Rows.Count == 0)
            {
                Msg = "Numero ordine non presente.";
                return false;
            }
            else
            {

                try
                {
                    SqlCommand sCmd = new SqlCommand("ReturnCompMMcons");
                    sCmd.Connection = dbHelper.cnSQL;
                    sCmd.CommandType = CommandType.Text;
                    sCmd.CommandText = @"
BEGIN TRAN;
declare @NewId as int
select @NewId= LastId + 1 
from MA_IDNumbers
where CodeType=3801093;

update MA_IDNumbers 
set LastId= @NewId
where CodeType=3801093;

insert into MA_InventoryEntries 
( PreprintedDocNo
, InvRsn
, PostingDate
, DocumentDate
, StoragePhase1
, Specificator1Type
, SpecificatorPhase1
, StoragePhase2
, ReceiptPhase1
, Currency
, EntryId
, AccrualDate
, CustSuppType
, CustSupp
, DocNo
, StubBook
, FromManufacturing
, Notes
 ) 
VALUES
( ''--PreprintedDocNo
, 'RCCONS'--InvRsn
, CONVERT(DATE,GETDATE())--PostingDate
, CONVERT(DATE,GETDATE())--DocumentDate
, 'BBOFFPRD'--StoragePhase1
, 6750211--Specificator1Type
, ''--SpecificatorPhase1
, ''--StoragePhase2
, 0--ReceiptPhase1
, 'EUR'--Currency
, @NewId -- EntryId
, CONVERT(DATE,GETDATE())--AccrualDate
, 6094850--CustSuppType
, ''--CustSupp
, convert(varchar,@NewId)--DocNo
, 'PR'--StubBook
, '0'--FromManufacturing
, ''--Notes
);

";
                    for (int r = 0; r < dti.Rows.Count; r++)
                    {
                        DataRow dri = dti.Rows[r];
                        sCmd.CommandText += @"
insert into MA_InventoryEntriesDetail 
(PostingDate
, Item
, UoM
, Qty
, Lot
, EntryId
, Line
, AccrualDate
, EntryTypeForLFBatchEval
, SubId
, BaseUoMQty
, Location
, UnitValue
, Discount1
, Discount2
, DiscountFormula
, DiscountAmount
, LineAmount
, InvRsn
, Description
, Moid
, MONo
, RtgStep
, MOCompLine ) 
VALUES
(  CONVERT(DATE,GETDATE())--PostingDate
, '";

                        sCmd.CommandText += Convert.ToString(dri["ITEM"]);
                        sCmd.CommandText += "','" + Convert.ToString(dri["BaseUoM"]) + "'," + Convert.ToString(dri["QTY"]) + ",'" + Convert.ToString(dri["BATCH"]);
                        sCmd.CommandText += @"'
, @NewId -- EntryId
, ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @",  CONVERT(DATE,GETDATE())--AccrualDate
, 12255235--det.EntryTypeForLFBatchEval
,  ";
                        sCmd.CommandText += (r + 1).ToString();
                        sCmd.CommandText += @"
,  ";
                        sCmd.CommandText += Convert.ToString(dri["QTY"]);
                        sCmd.CommandText += @",''-- Location
, 0--UnitValue
, 0--Discount1
, 0--Discount2
, ''--DiscountFormula
, 0--DiscountAmount
, 0--LineAmount
, 'RCCONS'--InvRsn
, ''--Description
, 0--Moid
, ''--MONo
, 0--RtgStep
, 0--MOCompLine
);

";
                    }
                    sCmd.CommandText += @"
insert into  Exs_WriteMovMag 
(DocType, DocId, Status) 
VALUES 
('MOVMAG',@NewId,'INS');
COMMIT TRAN;
";
                    //   sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    //    sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Msg = ex.Message;
                    return false;
                }
                finally
                {
                    if (dbHelper != null)
                    {
                        dbHelper.ChiudiDb();
                        dbHelper = null;
                    }
                }
            }
            return true;
        }
        public Stream Coded(CodedReq req)
        {
            string requestStr = "";
            string wWS = "Coded";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = CodedImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                    //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool CodedImpl(CodedReq req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            if (req == null || req.data == null || req.requestId == null || req.requestDate == null || req.data.BATCH_FINAL_CODE == null 
                || req.data.BATCH_INITIAL_CODE == null || req.data.BATCH_RECORD_BBM == null || req.data.BOXES == null || req.data.FINAL_CODE == null || req.data.INITIAL_CODE == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            int MaxID = 1;
            string sqli = @"
 SELECT isnull(MAX(IDIMPORTAZIONE),0) AS IDMax
 FROM TP_BBM_Coded WITH (NOLOCK)
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            if (dti.Rows.Count == 1)
            {
                DataRow dri = dti.Rows[0];
                MaxID = Convert.ToInt32(dri["IDMax"]) + 1;
            }
            if (req == null || req.data == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            bool bOk = true;
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            try
            {             
                for (int i = 0; i < req.data.BOXES.Count; i++)
                {
                    if (req.data.BOXES[i].QTY_BOX == null || req.data.BOXES[i].ID == null || req.data.BOXES[i].NUM_BOX == null)
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                        transaction.Rollback();
                        return false;
                    }
                    if (bOk == true)
                    {
                        SqlCommand sCmdb = new SqlCommand("inscoded");
                        sCmdb.Connection = dbHelper.cnSQL;
                        sCmdb.CommandType = CommandType.Text;
                        sCmdb.Transaction = transaction;
                        sCmdb.CommandText = @"
INSERT INTO TP_BBM_Coded
           (ID
           ,BATCH_RECORD_BBM
           ,FINAL_CODE
           ,BATCH_FINAL_CODE
           ,INITIAL_CODE
           ,BATCH_INITIAL_CODE
           ,NUM_BOX
           ,QTY_BOX
           ,IDIMPORTAZIONE
			)
     VALUES
           (@ID
           ,@BATCH_RECORD_BBM
           ,@FINAL_CODE
           ,@BATCH_FINAL_CODE
           ,@INITIAL_CODE
           ,@BATCH_INITIAL_CODE
           ,@NUM_BOX
           ,@QTY_BOX
           ,@IDIMPORTAZIONE
           )
";
                        sCmdb.Parameters.Add("@ID", SqlDbType.Int);
                        sCmdb.Parameters["@ID"].Value = req.data.BOXES[i].ID;
                        sCmdb.Parameters.Add("@BATCH_RECORD_BBM", SqlDbType.VarChar, 10);
                        sCmdb.Parameters["@BATCH_RECORD_BBM"].Value = req.data.BATCH_RECORD_BBM;
                        sCmdb.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmdb.Parameters.Add("@BATCH_FINAL_CODE", SqlDbType.VarChar, 16);
                        sCmdb.Parameters["@BATCH_FINAL_CODE"].Value = req.data.BATCH_FINAL_CODE;
                        sCmdb.Parameters.Add("@INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@INITIAL_CODE"].Value = req.data.INITIAL_CODE;
                        sCmdb.Parameters.Add("@BATCH_INITIAL_CODE", SqlDbType.VarChar, 16);
                        sCmdb.Parameters["@BATCH_INITIAL_CODE"].Value = req.data.BATCH_INITIAL_CODE;
                        sCmdb.Parameters.Add("@NUM_BOX", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@NUM_BOX"].Value = req.data.BOXES[i].NUM_BOX;
                        sCmdb.Parameters.Add("@QTY_BOX", SqlDbType.Int);
                        sCmdb.Parameters["@QTY_BOX"].Value = req.data.BOXES[i].QTY_BOX;
                        sCmdb.Parameters.Add("@IDIMPORTAZIONE", SqlDbType.Int);
                        sCmdb.Parameters["@IDIMPORTAZIONE"].Value = MaxID;
                        var readerb = sCmdb.ExecuteNonQuery();
                     
                    }
                }
                transaction.Commit();                
            }
            catch (Exception ex)
            {
                Msg = ex.Message;
                transaction.Rollback();
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }
            return true;
        }
        public Stream Serials(SerialsReq req)
        {
            string requestStr = "";
            string wWS = "Serials";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            MessageProperties props = OperationContext.Current.IncomingMessageProperties;
            RemoteEndpointMessageProperty prop = (RemoteEndpointMessageProperty)props[RemoteEndpointMessageProperty.Name];
            string addr = prop.Address;
            if (s.CheckIP == true)
            {
                if (CheckIPPT(addr) == false)
                {
                    string respId = Guid.NewGuid().ToString();
                    var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"403\",\"message\":\"IP richiedente non autorizzato\",\"data\":\"\"}";
                    byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "403"; //"403 Forbidden [IP richiedente non autorizzato]";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.Forbidden;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                    // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    return new MemoryStream(resultBytes);
                }
            }
            try
            {
                string msg = string.Empty;
                bool retVal = SerialsImpl(req, ref msg);
                byte[] resultBytes;
                if (retVal)
                {
                    string respId = Guid.NewGuid().ToString();
                    string wrisp = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"200\",\"message\":\"OK\",\"data\":" + JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)) + "}";
                    string responseStr = wrisp;
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    byte[] hash = new byte[0];
                    using (SHA512 shaM = new SHA512Managed())
                    {
                        hash = shaM.ComputeHash(resultBytes);
                    }
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                    //if (responseStr != "[]")
                    //{
                    WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200";// "200 OK";
                    WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                else
                {
                    var responseStr = "{\"error\":true}";
                    string respId = Guid.NewGuid().ToString();
                    if (msg != "Articolo non trovato.")
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"400\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    else
                    {
                        responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"406\",\"message\":\"" + msg + "\",\"data\":\"\"}";
                    }
                    resultBytes = Encoding.UTF8.GetBytes(responseStr);
                    WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                    if (msg != "Articolo non trovato.")
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "400";//"400 Bad Request";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.BadRequest;
                    }
                    else
                    {
                        WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "406";//"406 Not Acceptable";
                        WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.NotAcceptable;
                    }
                    //  WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                    InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                }
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {

                //    SDLog.LogFailRequest(requestStr, null, ex);
                string respId = Guid.NewGuid().ToString();
                var responseStr = "{\"requestId\":\"" + req.requestId + "\",\"responseId\":\"" + respId + "\",\"status\":\"500\",\"message\":\"" + ex.Message + "\",\"data\":\"\"}";
                //"{\"error\":true}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLogBBM(JsonConvert.SerializeObject(JsonConvert.SerializeObject(req)).Replace("\\", ""), responseStr, wWS, req.requestId, respId);
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private bool SerialsImpl(SerialsReq req, ref string Msg)
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);
            int MaxID = 1;
            string sqli = @"
 SELECT isnull(MAX(IDIMPORTAZIONE),0) AS IDMax
 FROM TP_BBM_Serials WITH (NOLOCK)
";
            SqlDataAdapter dai = new SqlDataAdapter(sqli, dbHelper.cnSQL);
            SqlCommand cmdi = dai.SelectCommand;
            DataSet dsi = new DataSet("dsi");
            string idTabi = "reci";
            dai.Fill(dsi, idTabi);
            DataTable dti = dsi.Tables[0];
            if (dti.Rows.Count == 1)
            {
                DataRow dri = dti.Rows[0];
                MaxID = Convert.ToInt32(dri["IDMax"]) + 1;
            }
            if (req == null || req.data == null)
            {
                Msg = "Struttura non corretta.";
                return false;
            }
            bool bOk = true;
            SqlTransaction transaction = dbHelper.cnSQL.BeginTransaction();
            try
            {

                if (req.data.BATCH_FINAL_CODE == null || req.data.BATCH_INITIAL_CODE == null || req.data.BATCH_RECORD_BBM == null
                || req.data.SERIALS == null || req.data.FINAL_CODE == null || req.data.INITIAL_CODE == null
                || req.data.PRODUCT_CODE_FINAL_CODE == null || req.data.PRODUCT_CODE_INITIAL_CODE == null)
                {
                    Msg = "Struttura non corretta.";
                    bOk = false;
                    transaction.Rollback();
                    return false;
                }
                for (int i = 0; i < req.data.SERIALS.Count; i++)
                {
                    if (req.data.SERIALS[i].DATAMATRIX_RAW == null || req.data.SERIALS[i].ID == null || req.data.SERIALS[i].NUM_BOX == null || req.data.SERIALS[i].SERIAL_NUMBER_FINAL_CODE == null)
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                        transaction.Rollback();
                        return false;
                    }
                    if (bOk == true)
                    {
                        SqlCommand sCmdb = new SqlCommand("insserials");
                        sCmdb.Connection = dbHelper.cnSQL;
                        sCmdb.CommandType = CommandType.Text;
                        sCmdb.Transaction = transaction;
                        sCmdb.CommandText = @"
INSERT INTO TP_BBM_Serials
           (BATCH_RECORD_BBM
           ,FINAL_CODE
           ,PRODUCT_CODE_FINAL_CODE
           ,BATCH_FINAL_CODE
           ,INITIAL_CODE
           ,PRODUCT_CODE_INITIAL_CODE
           ,BATCH_INITIAL_CODE
           ,SERIAL_NUMBER_FINAL_CODE
           ,DATAMATRIX_RAW
           ,NUM_BOX
           ,ID
           ,IDIMPORTAZIONE
           ,PPN
           ,GTIN
           ,BATCH
           ,EXPIRYDATE
           ,SERIAL
           ,DTM_TYPE
            )
     VALUES
           (@BATCH_RECORD_BBM
           ,@FINAL_CODE
           ,@PRODUCT_CODE_FINAL_CODE
           ,@BATCH_FINAL_CODE
           ,@INITIAL_CODE
           ,@PRODUCT_CODE_INITIAL_CODE
           ,@BATCH_INITIAL_CODE
           ,@SERIAL_NUMBER_FINAL_CODE
           ,@DATAMATRIX_RAW
           ,@NUM_BOX
           ,@ID
           ,@IDIMPORTAZIONE
           ,@PPN
           ,@GTIN
           ,@BATCH
           ,@EXPIRYDATE
           ,@SERIAL
           ,@DTM_TYPE
           )
";
                        sCmdb.Parameters.Add("@BATCH_RECORD_BBM", SqlDbType.VarChar, 10);
                        sCmdb.Parameters["@BATCH_RECORD_BBM"].Value = req.data.BATCH_RECORD_BBM;
                        sCmdb.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmdb.Parameters.Add("@PRODUCT_CODE_FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@PRODUCT_CODE_FINAL_CODE"].Value = req.data.PRODUCT_CODE_FINAL_CODE;
                        sCmdb.Parameters.Add("@BATCH_FINAL_CODE", SqlDbType.VarChar, 16);
                        sCmdb.Parameters["@BATCH_FINAL_CODE"].Value = req.data.BATCH_FINAL_CODE;
                        sCmdb.Parameters.Add("@INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@INITIAL_CODE"].Value = req.data.INITIAL_CODE;
                        sCmdb.Parameters.Add("@PRODUCT_CODE_INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@PRODUCT_CODE_INITIAL_CODE"].Value = req.data.PRODUCT_CODE_INITIAL_CODE;
                        sCmdb.Parameters.Add("@BATCH_INITIAL_CODE", SqlDbType.VarChar, 16);
                        sCmdb.Parameters["@BATCH_INITIAL_CODE"].Value = req.data.BATCH_INITIAL_CODE;
                        sCmdb.Parameters.Add("@SERIAL_NUMBER_FINAL_CODE", SqlDbType.VarChar, 20);
                        sCmdb.Parameters["@SERIAL_NUMBER_FINAL_CODE"].Value = req.data.SERIALS[i].SERIAL_NUMBER_FINAL_CODE;
                        sCmdb.Parameters.Add("@DATAMATRIX_RAW", SqlDbType.VarChar, 4000);
                        sCmdb.Parameters["@DATAMATRIX_RAW"].Value = string.Join(",", req.data.SERIALS[i].DATAMATRIX_RAW);
                        sCmdb.Parameters.Add("@NUM_BOX", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@NUM_BOX"].Value = req.data.SERIALS[i].NUM_BOX;
                        sCmdb.Parameters.Add("@ID", SqlDbType.Int);
                        sCmdb.Parameters["@ID"].Value = req.data.SERIALS[i].ID;
                        sCmdb.Parameters.Add("@IDIMPORTAZIONE", SqlDbType.Int);
                        sCmdb.Parameters["@IDIMPORTAZIONE"].Value = MaxID;

                        byte[] data = FileUtil.FromHex(req.data.SERIALS[i].DATAMATRIX_RAW);
                        string dm = Encoding.ASCII.GetString(data);
                        string dPPN;
                        string dGTIN;
                        string dBATCH;
                        string dEXPIRYDATE;
                        string dSERIAL;
                        string dDTM_TYPE;
                        if (dm.Length > 0)
                        {
                            if ((dm.Substring(0, 4) == "[)>\u001e") || (dm.Substring(0, 5) == "[)>RS"))
                            {
                                //IFA
                                IFADatamatrixParser prs = new IFADatamatrixParser();
                                IFADatamatrix ifadm = prs.TryParseIFADatamatrix(dm);
                                if (ifadm != null)
                                {
                                    dPPN = ifadm.ppn;
                                    dGTIN = ifadm.gtin;
                                    dBATCH = ifadm.batch;
                                    dEXPIRYDATE = ifadm.expirationDate;
                                    dSERIAL = ifadm.serialNumber;
                                    dDTM_TYPE = "IFA";
                                }
                                else
                                {
                                    dPPN = "";
                                    dGTIN = "";
                                    dBATCH = "";
                                    dEXPIRYDATE = "";
                                    dSERIAL = "";
                                    dDTM_TYPE = "IFANO";
                                }
                            }
                            else
                            {
                                //GS1
                                GS1DatamatrixParser prs = new GS1DatamatrixParser();
                                GS1Datamatrix gs1dm = prs.TryParseGS1Datamatrix(dm);
                                if (gs1dm != null)
                                {
                                    dPPN = string.Empty;
                                    dGTIN = gs1dm.gtin;
                                    dBATCH = gs1dm.batch;
                                    dEXPIRYDATE = gs1dm.expirationDate;
                                    dSERIAL = gs1dm.serialNumber;
                                    dDTM_TYPE = "GS1";
                                }
                                else
                                {

                                    dPPN = "";
                                    dGTIN = "";
                                    dBATCH = "";
                                    dEXPIRYDATE = "";
                                    dSERIAL = "";
                                    dDTM_TYPE = "GS1NO";
                                }
                            }
                        }
                        else
                        {
                            dPPN = "";
                            dGTIN = "";
                            dBATCH = "";
                            dEXPIRYDATE = "";
                            dSERIAL = "";
                            dDTM_TYPE = "NONE";
                        }
                        sCmdb.Parameters.Add("@PPN", SqlDbType.VarChar, 50);
                        sCmdb.Parameters["@PPN"].Value = dPPN;
                        sCmdb.Parameters.Add("@GTIN", SqlDbType.VarChar, 50);
                        sCmdb.Parameters["@GTIN"].Value = dGTIN;
                        sCmdb.Parameters.Add("@BATCH", SqlDbType.VarChar, 30);
                        sCmdb.Parameters["@BATCH"].Value = dBATCH;
                        sCmdb.Parameters.Add("@EXPIRYDATE", SqlDbType.VarChar, 50);
                        sCmdb.Parameters["@EXPIRYDATE"].Value = dEXPIRYDATE;
                        sCmdb.Parameters.Add("@SERIAL", SqlDbType.VarChar, 50);
                        sCmdb.Parameters["@SERIAL"].Value = dSERIAL;
                        sCmdb.Parameters.Add("@DTM_TYPE", SqlDbType.VarChar, 5);
                        sCmdb.Parameters["@DTM_TYPE"].Value = dDTM_TYPE;
                        var readerb = sCmdb.ExecuteNonQuery();
                        string sqlupdit = @"
update pt
set INITIAL_CODE=isnull(tr.ItemMago,pt.INITIAL_CODE)
,[PRODUCT_CODE_FINAL_CODE_SCHEME] = isnull((SELECT TOP 1 pmd.ProductCodeSchemeStr FROM TP_Emvo_ProductMasterData AS pmd WITH (NOLOCK) WHERE pmd.ProductCode = pt.[PRODUCT_CODE_FINAL_CODE]),'')
,[BATCH_FINAL_CODE_EXPIRY_DATE] = isnull((SELECT TOP 1 lot.ValidTo FROM MA_Lots AS lot  WITH (NOLOCK) WHERE lot.Item = pt.[FINAL_CODE] AND lot.Lot = pt.[BATCH_FINAL_CODE]),'17991231')
,[PACK_SIZE] = isnull((SELECT TOP 1 pmd.PackSize FROM TP_Emvo_ProductMasterData AS pmd WITH (NOLOCK) WHERE pmd.ProductCode = pt.[PRODUCT_CODE_FINAL_CODE]),0)
,[PRODUCT_CODE_INITIAL_CODE_SCHEME] = case when (SELECT TOP 1 it.TipoCodice FROM TP_ItemPPN AS it WITH (NOLOCK) WHERE it.Item = isnull(tr.ItemMago,pt.INITIAL_CODE) and it.PPN=pt.PRODUCT_CODE_INITIAL_CODE) is  null then
isnull((SELECT TOP 1 it.TipoCodice FROM MA_Items AS it WITH (NOLOCK) WHERE it.Item = isnull(tr.ItemMago,pt.INITIAL_CODE)),'')
else
(SELECT TOP 1 it.TipoCodice FROM TP_ItemPPN  AS it WITH (NOLOCK) WHERE it.Item =isnull(tr.ItemMago,pt.INITIAL_CODE) and it.PPN=pt.PRODUCT_CODE_INITIAL_CODE) 
end
from 
TP_BBM_Serials as pt WITH (NOLOCK)
left outer join TP_TranscodeItem as tr WITH (NOLOCK)
on tr.Item=pt.INITIAL_CODE
WHERE pt.IDIMPORTAZIONE=@IdImp;
";
                        SqlCommand cmdupdit = new SqlCommand(sqlupdit, dbHelper.cnSQL);
                        cmdupdit.Transaction = transaction;
                        SqlParameter parupdit = cmdupdit.Parameters.Add("@IdImp", SqlDbType.Int);
                        parupdit.Direction = ParameterDirection.Input;
                        parupdit.Value = MaxID;
                        cmdupdit.CommandTimeout = 1200;
                        cmdupdit.ExecuteNonQuery();
                        //                        string sqlBX = @"
                        //SELECT *
                        //  FROM TP_Box
                        //  WHERE BoxCode=@BoxCode
                        //";
                        //                        SqlDataAdapter daBX = new SqlDataAdapter(sqlBX, dbHelper.cnSQL);
                        //                        SqlCommand cmdBX = daBX.SelectCommand;
                        //                        SqlParameter parBX = cmdBX.Parameters.Add("@BoxCode", SqlDbType.VarChar, 30);
                        //                        parBX.Direction = ParameterDirection.Input;
                        //                        parBX.Value = req.data.BOXES[i].NUM_BOX;
                        //                        DataSet dsBX = new DataSet("dsBX");
                        //                        string idTabBX = "recBX";
                        //                        daBX.Fill(dsBX, idTabBX);
                        //                        DataTable dtBX = dsBX.Tables[0];
                        //                        if (dtBX.Rows.Count == 0)
                        //                        {
                        //                            Msg = "Scatola non trovata.";
                        //                            bOk = false;
                        //                        }
                    }
                }

                /*
                string sqlck = @"
SELECT *
  FROM TP_BBM_ReturnCompBoxes
  WHERE NUM_ORDER=@NUM_ORDER
";
                SqlDataAdapter dack = new SqlDataAdapter(sqlck, dbHelper.cnSQL);
                SqlCommand cmdck = dack.SelectCommand;
                SqlParameter parck = cmdck.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                parck.Direction = ParameterDirection.Input;
                parck.Value = req.data.NUM_ORDER;
                DataSet dsck = new DataSet("dsck");
                string idTabck = "recck";
                dack.Fill(dsck, idTabck);
                DataTable dtck = dsck.Tables[0];

                if (dtck.Rows.Count != req.data.BOXES.Count || bOk == false)
                {
                    if (bOk == true) Msg = "Errore inserimento righe scatole.";
                    SqlCommand sCmdDb = new SqlCommand("retcompboxdel");
                    sCmdDb.Connection = dbHelper.cnSQL;
                    sCmdDb.CommandType = CommandType.Text;
                    sCmdDb.CommandText = @"
DELETE FROM TP_BBM_ReturnCompBoxes
  WHERE NUM_ORDER=@NUM_ORDER
";
                    sCmdDb.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    sCmdDb.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var readerDb = sCmdDb.ExecuteNonQuery();
                    return false;
                }


                for (int i = 0; i < req.data.ITEMS.Count; i++)
                {
                    if (req.data.ITEMS[i].BATCH == null || req.data.ITEMS[i].INITIAL_CODE == null || req.data.ITEMS[i].QTY_RETURNED == null || req.data.ITEMS[i].TYPE == null || req.data.ITEMS[i].QTY_WASTED == null)
                    {
                        Msg = "Struttura non corretta.";
                        bOk = false;
                    }
                    if (bOk == true)
                    {
                        SqlCommand sCmd = new SqlCommand("retcompitemimpl");
                        sCmd.Connection = dbHelper.cnSQL;
                        sCmd.CommandType = CommandType.Text;
                        sCmd.CommandText = @"
INSERT INTO TP_BBM_ReturnCompItems
           (NUM_ORDER
           ,FINAL_CODE
           ,QTY_FINAL
           ,NUM_CARTS
           ,INITIAL_CODE
           ,BATCH
           ,QTY_RETURNED
           ,TYPE
           ,FRIDGE
           ,QTY_WASTED
           ,PROCESSED)
     VALUES
           (@NUM_ORDER
           ,@FINAL_CODE
           ,@QTY_FINAL
           ,@NUM_CARTS
           ,@INITIAL_CODE
           ,@BATCH
           ,@QTY_RETURNED
           ,@TYPE
           ,@FRIDGE
           ,@QTY_WASTED
           ,'0')
";
                        sCmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                        sCmd.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                        sCmd.Parameters.Add("@FINAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@FINAL_CODE"].Value = req.data.FINAL_CODE;
                        sCmd.Parameters.Add("@QTY_FINAL", SqlDbType.Float);
                        sCmd.Parameters["@QTY_FINAL"].Value = req.data.QTY_FINAL;
                        sCmd.Parameters.Add("@NUM_CARTS", SqlDbType.VarChar, 4000);
                        sCmd.Parameters["@NUM_CARTS"].Value = string.Join(",", req.data.NUM_CARTS);
                        sCmd.Parameters.Add("@INITIAL_CODE", SqlDbType.VarChar, 30);
                        sCmd.Parameters["@INITIAL_CODE"].Value = req.data.ITEMS[i].INITIAL_CODE;
                        sCmd.Parameters.Add("@BATCH", SqlDbType.VarChar, 16);
                        sCmd.Parameters["@BATCH"].Value = req.data.ITEMS[i].BATCH;
                        sCmd.Parameters.Add("@QTY_RETURNED", SqlDbType.Float);
                        sCmd.Parameters["@QTY_RETURNED"].Value = req.data.ITEMS[i].QTY_RETURNED;
                        sCmd.Parameters.Add("@TYPE", SqlDbType.VarChar, 9);
                        sCmd.Parameters["@TYPE"].Value = req.data.ITEMS[i].TYPE;
                        sCmd.Parameters.Add("@FRIDGE", SqlDbType.VarChar, 9);
                        sCmd.Parameters["@FRIDGE"].Value = req.data.FRIDGE;
                        sCmd.Parameters.Add("@QTY_WASTED", SqlDbType.Float);
                        sCmd.Parameters["@QTY_WASTED"].Value = req.data.ITEMS[i].QTY_WASTED;
                        var reader = sCmd.ExecuteNonQuery();
                        string sqlF = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
                        SqlDataAdapter daF = new SqlDataAdapter(sqlF, dbHelper.cnSQL);
                        SqlCommand cmdF = daF.SelectCommand;
                        SqlParameter parF = cmdF.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parF.Direction = ParameterDirection.Input;
                        parF.Value = req.data.FINAL_CODE;
                        DataSet dsF = new DataSet("dsF");
                        string idTabF = "recF";
                        daF.Fill(dsF, idTabF);
                        DataTable dtF = dsF.Tables[0];

                        if (dtF.Rows.Count == 0)
                        {
                            Msg = "Articolo non trovato.";
                            bOk = false;
                        }

                        string sqlA = @"
SELECT *
  FROM MA_Items
  WHERE Item=@Item
";
                        SqlDataAdapter daA = new SqlDataAdapter(sqlA, dbHelper.cnSQL);
                        SqlCommand cmdA = daA.SelectCommand;
                        SqlParameter parA = cmdA.Parameters.Add("@Item", SqlDbType.VarChar, 21);
                        parA.Direction = ParameterDirection.Input;
                        parA.Value = req.data.ITEMS[i].INITIAL_CODE;
                        DataSet dsA = new DataSet("dsA");
                        string idTabA = "recA";
                        daA.Fill(dsA, idTabA);
                        DataTable dtA = dsA.Tables[0];

                        if (dtA.Rows.Count == 0)
                        {
                            Msg = "Componente non trovato.";
                            bOk = false;
                        }
                    }
                }

                string sql = @"
SELECT *
  FROM TP_BBM_ReturnCompItems
  WHERE NUM_ORDER=@NUM_ORDER
";
                SqlDataAdapter da = new SqlDataAdapter(sql, dbHelper.cnSQL);
                SqlCommand cmd = da.SelectCommand;
                SqlParameter par = cmd.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                par.Direction = ParameterDirection.Input;
                par.Value = req.data.NUM_ORDER;
                DataSet ds = new DataSet("ds");
                string idTab = "rec";
                da.Fill(ds, idTab);
                DataTable dt = ds.Tables[0];

                if (dt.Rows.Count != req.data.ITEMS.Count || bOk == false)
                {
                    if (bOk == true) Msg = "Errore inserimento righe articoli.";
                    SqlCommand sCmdD = new SqlCommand("retcompitemdel");
                    sCmdD.Connection = dbHelper.cnSQL;
                    sCmdD.CommandType = CommandType.Text;
                    sCmdD.CommandText = @"
DELETE FROM TP_BBM_ReturnCompItems
  WHERE NUM_ORDER=@NUM_ORDER
";
                    sCmdD.Parameters.Add("@NUM_ORDER", SqlDbType.VarChar, 10);
                    sCmdD.Parameters["@NUM_ORDER"].Value = req.data.NUM_ORDER;
                    var reader = sCmdD.ExecuteNonQuery();
                    return false;
                };*/
                transaction.Commit();
            }

            catch (Exception ex)
            {
                Msg = ex.Message;
                transaction.Rollback();
                return false;
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return true;
        }
        #endregion
        public System.IO.Stream ListaArticoli()
        {
            string requestStr = "";
            JsonSettings.SetBaseDirPath(HttpRuntime.AppDomainAppPath);
            JsonSettings s = JsonSettings.Instance;
            try
            {
                List<Articolo> retVal = ListaArticoliImpl();
                string responseStr = JsonConvert.SerializeObject(retVal);

                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                byte[] hash = new byte[0];
                using (SHA512 shaM = new SHA512Managed())
                {
                    hash = shaM.ComputeHash(resultBytes);
                }
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Hash"] = BitConverter.ToString(hash).Replace("-", "");
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "200 OK";
                InsertLog("IN", BitConverter.ToString(hash).Replace("-", ""), "200 OK");
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.OK;
                return new MemoryStream(resultBytes);
            }
            catch (Exception ex)
            {
                SDLog.LogFailRequest(requestStr, null, ex);
                var responseStr = "{\"error\":true,\"ErrDescr\":\"" + ex.Message + "\"}";
                byte[] resultBytes = Encoding.UTF8.GetBytes(responseStr);
                WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                WebOperationContext.Current.OutgoingResponse.Headers["Status"] = "500 Internal Server Error";
                WebOperationContext.Current.OutgoingResponse.StatusCode = System.Net.HttpStatusCode.InternalServerError;
                InsertLog("IN", "", "500 Internal Server Error");
                // WebOperationContext.Current.OutgoingResponse.ContentType = "application/json; charset=UTF-8";
                return new MemoryStream(resultBytes);
            }
        }
        private List<Articolo> ListaArticoliImpl()
        {
            JsonSettings s = JsonSettings.Instance;
            DbHelper.DbConnection dbConn = new DbHelper.DbConnection();
            dbConn.DbString = s.DbString + Decrypt(s.Password, "TP-Italia");
            dbConn.DbSqlConnection = s.DbSqlConnection + Decrypt(s.Password, "TP-Italia");
            DbHelper dbHelper = new DbHelper();
            dbHelper.ApriDb(dbConn);

            List<Articolo> artList = new List<Articolo>();

            try
            {

                SqlCommand sCmd = new SqlCommand("getcustomer");
                sCmd.Connection = dbHelper.cnSQL;
                sCmd.CommandType = CommandType.Text;
                sCmd.CommandText = @"
SELECT [Item], [Description]
FROM [MA_Items]
ORDER BY [Item]
";

                var reader = sCmd.ExecuteReader();
                while (reader.Read())
                {
                    Articolo art = new Articolo();
                    art.Item = reader["Item"].ToString();
                    art.Description = reader["Description"].ToString();
                    artList.Add(art);
                }
            }
            finally
            {
                if (dbHelper != null)
                {
                    dbHelper.ChiudiDb();
                    dbHelper = null;
                }
            }

            return artList;
        }

    }
    
}

