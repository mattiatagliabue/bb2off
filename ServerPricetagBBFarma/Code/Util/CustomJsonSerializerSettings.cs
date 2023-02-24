using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ServerPricetagBBFarma.Code.Util
{
    public class CustomJsonSerializerSettings : JsonSerializerSettings
    {
        private static CustomJsonSerializerSettings instance = null;

        public static CustomJsonSerializerSettings Instance
        {
            get
            {
                if (instance == null)
                {
                    CustomJsonSerializerSettings.instance = new CustomJsonSerializerSettings();
                }
                return instance;
            }
        }

        private CustomJsonSerializerSettings()
        {
            this.Error = this.HandleDeserializationError;
        }

        public void HandleDeserializationError(object sender, ErrorEventArgs errorArgs)
        {
            errorArgs.ErrorContext.Handled = true;
            var currentObj = errorArgs.CurrentObject as DateTime?;

            if (currentObj == null) return;
            currentObj = new DateTime(1980, 1, 1);
        }
    }
}