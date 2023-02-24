using System.Collections.Generic;

namespace SDCommon.Barcode
{
    public class GS1DatamatrixParser
    {
        public GS1Datamatrix TryParseGS1Datamatrix(string rawBarcodeData)
        {
            if (rawBarcodeData == null)
            {
                return null;
            }

            if (rawBarcodeData.Length == 0)
            {
                return null;
            }

            HashSet<string> symbologyIdentifier = new HashSet<string>();
            symbologyIdentifier.Add("]C1");
            symbologyIdentifier.Add("]e0");
            symbologyIdentifier.Add("]d2");
            symbologyIdentifier.Add("]Q3");
            bool symIdRemoved = false;
            foreach (string symId in symbologyIdentifier)
            {
                if (rawBarcodeData.StartsWith(symId))
                {
                    if (symIdRemoved == false)
                    {
                        rawBarcodeData = rawBarcodeData.Substring(symId.Length);
                        symIdRemoved = true;
                    }
                }
            }

            GS1Datamatrix gs1dm = ParseGS1Datamatrix(rawBarcodeData);

            if (gs1dm != null)
            {
                if (gs1dm.gtin.Length > 0
                        && gs1dm.batch.Length > 0
                        && gs1dm.serialNumber.Length > 0
                        && (
                        gs1dm.expirationDate.Length > 0
                                || gs1dm.bestBeforeDate.Length > 0
                                || gs1dm.productionDate.Length > 0
                ))
                {
                    return gs1dm;
                }
            }

            return null;
        }

        public GS1Datamatrix ParseGS1Datamatrix(string rawBarcodeData)
        {
            HashSet<char> separators = new HashSet<char>();
            separators.Add(' ');
            separators.Add((char)29); // group separator

            Dictionary<string, int> fixedSizeEl = new Dictionary<string, int>();
            fixedSizeEl.Add("00", 20);
            fixedSizeEl.Add("01", 16);
            fixedSizeEl.Add("02", 16);
            fixedSizeEl.Add("03", 16);
            fixedSizeEl.Add("04", 18);
            fixedSizeEl.Add("11", 8);
            fixedSizeEl.Add("12", 8);
            fixedSizeEl.Add("13", 8);
            fixedSizeEl.Add("14", 8);
            fixedSizeEl.Add("15", 8);
            fixedSizeEl.Add("16", 8);
            fixedSizeEl.Add("17", 8);
            fixedSizeEl.Add("18", 8);
            fixedSizeEl.Add("19", 8);
            fixedSizeEl.Add("20", 4);
            fixedSizeEl.Add("31", 10);
            fixedSizeEl.Add("32", 10);
            fixedSizeEl.Add("33", 10);
            fixedSizeEl.Add("34", 10);
            fixedSizeEl.Add("35", 10);
            fixedSizeEl.Add("36", 10);
            fixedSizeEl.Add("41", 16);

            Dictionary<string, string> data = ParseGS1DatamatrixInt(rawBarcodeData.ToCharArray(), 0, fixedSizeEl, separators);

            if (data == null)
            {
                return null;
            }
            else
            {
                GS1Datamatrix dm = new GS1Datamatrix();

                dm.completeBarcode = rawBarcodeData;

                if (data.ContainsKey("01"))
                {
                    dm.gtin = data["01"];
                }

                if (data.ContainsKey("10"))
                {
                    dm.batch = data["10"];
                }

                if (data.ContainsKey("11"))
                {
                    dm.productionDate = data["11"];
                }

                if (data.ContainsKey("15"))
                {
                    dm.bestBeforeDate = data["15"];
                }

                if (data.ContainsKey("17"))
                {
                    dm.expirationDate = data["17"];
                }

                if (data.ContainsKey("21"))
                {
                    dm.serialNumber = data["21"];
                }

                return dm;
            }
        }

        private Dictionary<string, string> ParseGS1DatamatrixInt(
                char[] cs, int i,
                Dictionary<string, int> fixedSizeEl, HashSet<char> separators)
        {
            Dictionary<string, string> data = new Dictionary<string, string>();

            while (i < cs.Length)
            {
                if (i < (cs.Length - 3))
                { // app id + 1 carattere
                    string applicationId = new string(cs, i, 2);
                    if (fixedSizeEl.ContainsKey(applicationId))
                    {
                        // fixed size
                        int length = fixedSizeEl[applicationId];
                        string dataStr;
                        if (cs.Length >= (i + length))
                        {
                            dataStr = new string(cs, i + 2, length - 2);
                        }
                        else
                        {
                            return null;
                        }
                        if (data.ContainsKey(applicationId) == false)
                        {
                            data.Add(applicationId, dataStr);
                        }
                        i += length;
                    }
                    else
                    {
                        // variable size with separator
                        int separatorPos = i + 2;
                        while (separatorPos < cs.Length && separators.Contains(cs[separatorPos]) == false)
                        {
                            separatorPos++;
                        }
                        string dataStr = new string(cs, i + 2, (separatorPos - i - 2));
                        if (data.ContainsKey(applicationId) == false)
                        {
                            data.Add(applicationId, dataStr);
                        }
                        i = separatorPos + 1;
                    }
                }
                else
                {
                    // no application identifier!
                    return null;
                }
            }

            return data;
        }
    }
}