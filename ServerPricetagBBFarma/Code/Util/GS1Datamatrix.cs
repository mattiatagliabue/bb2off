using System;

namespace SDCommon.Barcode
{
    public class GS1Datamatrix
    {
        public GS1Datamatrix()
        {
            this.gtin = "";
            this.batch = "";
            this.productionDate = "";
            this.bestBeforeDate = "";
            this.expirationDate = "";
            this.serialNumber = "";
            this.completeBarcode = "";
        }

        public string gtin { get; set; }
        public string batch { get; set; }
        public string productionDate { get; set; }
        public string bestBeforeDate { get; set; }
        public string expirationDate { get; set; }
        public string serialNumber { get; set; }
        public string completeBarcode { get; set; }

        public static DateTime GetDateTimeFromStr(string dateStr, DateTime defaultDT)
        {
            // YYMMDD
            if (dateStr.Length == 6)
            {
                string yearStr = dateStr.Substring(0, 2);
                int year = int.Parse(yearStr);
                if (year < 100)
                {
                    year += 2000;
                }
                string monthStr = dateStr.Substring(2, 2);
                string dayStr = dateStr.Substring(4, 2);

                int month = int.Parse(monthStr);
                int day;
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
            return defaultDT;
        }
    }
}
