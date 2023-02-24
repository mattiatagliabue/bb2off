using System;

namespace SDCommon.Barcode
{
    public class IFADatamatrix
    {
        public IFADatamatrix()
        {
            this.gtin = "";
            this.ppn = "";
            this.batch = "";
            this.productionDate = "";
            this.expirationDate = "";
            this.serialNumber = "";
            this.completeBarcode = "";
            this.price = "";
        }

        public string gtin { get; set; }
        public string ppn { get; set; }
        public string batch { get; set; }
        public string productionDate { get; set; }
        public string expirationDate { get; set; }
        public string price { get; set; }
        public string serialNumber { get; set; }
        public string completeBarcode { get; set; }


        public static DateTime GetDateTimeFromStr(string dateStr, DateTime defaultDT)
        {
            if (string.IsNullOrEmpty(dateStr))
            {
                return defaultDT;
            }

            // YYMMDD
            if (dateStr.Length == 6)
            {
                try
                {
                    string yearStr = dateStr.Substring(0, 2);
                    int year = int.Parse(yearStr);
                    if (year >= 0 && year < 100)
                    {
                        year += 2000;
                    }
                    else
                    {
                        return defaultDT;
                    }
                    string monthStr = dateStr.Substring(2, 2);
                    string dayStr = dateStr.Substring(4, 2);

                    int month = int.Parse(monthStr);
                    if (!(month > 0))
                    {
                        return defaultDT;
                    }

                    int day = 0;
                    if (dayStr != "00")
                    {
                        day = int.Parse(dayStr);
                    }
                    else
                    {
                        day = DateTime.DaysInMonth(year, month);
                    }

                    return new DateTime(year, month, day);
                }
                catch (Exception)
                {
                    return defaultDT;
                }
            }
            return defaultDT;
        }
    }
}
