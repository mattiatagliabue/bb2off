using System.Runtime.CompilerServices;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Web.UI;
using static ServerPricetagBBFarma.ServerPricetagBBFarmaImpl;

namespace ServerPricetagBBFarma
{
    [ServiceContract]
    public interface IServerPricetagBBFarma
    {
        [OperationContract]
        //[WebInvoke(Method = "GET", UriTemplate = "lista_articoli")]
        //   System.IO.Stream ListaArticoli();
        [WebInvoke(Method = "POST", UriTemplate = "layout/select", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Select(SelectReq req);

        [WebInvoke(Method = "POST", UriTemplate = "layout/insert", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Insert(InsertReq req);

        [WebInvoke(Method = "POST", UriTemplate = "layout/update", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Update(UpdateReq req);

        [WebInvoke(Method = "POST", UriTemplate = "layout/delete", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Delete(DeleteReq req);

        [WebInvoke(Method = "POST", UriTemplate = "layout/deleteall", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream DeleteAll(DeleteAllReq req);

        [WebInvoke(Method = "POST", UriTemplate = "stayalivepost",RequestFormat =WebMessageFormat.Json,ResponseFormat =WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream StayAlivePOST(StayAliveReq req);

        [WebInvoke(Method = "GET", UriTemplate = "keepalive")]
        System.IO.Stream KeepAlive();

        [WebInvoke(Method = "GET", UriTemplate = "keepalive/{machineid}")]
        System.IO.Stream KeepAliveRW(string machineid);

        [WebInvoke(Method = "POST", UriTemplate = "rework", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Rework(ReworkReq req);

        [WebInvoke(Method = "POST", UriTemplate = "pickingrequest", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream PickingRequest(PickReq req);

        [WebInvoke(Method = "POST", UriTemplate = "pickingrefillrequest", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream PickingRefillRequest(PickReq req);

        [WebInvoke(Method = "POST", UriTemplate = "pickingabort", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream PickingAbort(PickAbort req);

        [WebInvoke(Method = "POST", UriTemplate = "pickingwrong", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream PickingWrong(PickWrong req);

        [WebInvoke(Method = "POST", UriTemplate = "palletready", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream PalletReady(PalletReadyReq req);

        [WebInvoke(Method = "POST", UriTemplate = "cartfree", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream CartFree(CartFreeReq req);

        [WebInvoke(Method = "POST", UriTemplate = "returncomponent", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream ReturnComponent(ReturnComponentReq req);

        [WebInvoke(Method = "POST", UriTemplate = "serials", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Serials(SerialsReq req);

        [WebInvoke(Method = "POST", UriTemplate = "coded", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream Coded(CodedReq req);

        [WebInvoke(Method = "POST", UriTemplate = "productrefused", RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.WrappedResponse)]
        System.IO.Stream ProductRefused(ProductRefusedReq req);

        [WebInvoke(Method = "GET", UriTemplate = "finalproducts/{deposito}")]
        System.IO.Stream FinalProducts(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "finalproducts/{deposito}/{inc}")]
        System.IO.Stream FinalProductsInc(string deposito,string inc);

        [WebInvoke(Method = "GET", UriTemplate = "initialcomponents/{deposito}")]
        System.IO.Stream InitialComponents(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "initialcomponents/{deposito}/{inc}")]
        System.IO.Stream InitialComponentsInc(string deposito, string inc);

        [WebInvoke(Method = "GET", UriTemplate = "baselists/{deposito}")]
        System.IO.Stream BaseLists(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "baselists/{deposito}/{inc}")]
        System.IO.Stream BaseListsInc(string deposito, string inc);

        [WebInvoke(Method = "GET", UriTemplate = "deliveryrepacking/{deposito}/{giorni}")]
        System.IO.Stream DeliveryPacking(string deposito, string giorni);

        [WebInvoke(Method = "GET", UriTemplate = "deliveryrepacking/{deposito}")]
        System.IO.Stream DeliveryPackingNoDays(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "mah")]
        System.IO.Stream Mah();

        [WebInvoke(Method = "GET", UriTemplate = "mah/{inc}")]
        System.IO.Stream MahInc(string inc);

        [WebInvoke(Method = "GET", UriTemplate = "producers")]
        System.IO.Stream Producers();

        [WebInvoke(Method = "GET", UriTemplate = "producers/{inc}")]
        System.IO.Stream ProducersInc(string inc);

        [WebInvoke(Method = "GET", UriTemplate = "finalproductbatch")]
        System.IO.Stream FinalProductBatch();

        [WebInvoke(Method = "GET", UriTemplate = "finalproductbatch/{deposito}")]
        System.IO.Stream FinalProductBatchOff(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "finalproductbatch/{deposito}/{inc}")]
        System.IO.Stream FinalProductBatchOffInc(string deposito, string inc);

        [WebInvoke(Method = "GET" , UriTemplate = "leafletversions")]
        System.IO.Stream LeafLetVersions();

        [WebInvoke(Method = "GET", UriTemplate = "leafletversions/{deposito}")]
        System.IO.Stream LeafLetVersionsOff(string deposito);

        [WebInvoke(Method = "GET", UriTemplate = "leafletversions/{deposito}/{inc}")]
        System.IO.Stream LeafLetVersionsOffInc(string deposito, string inc);

        [WebInvoke(Method = "GET", UriTemplate = "productcodes")]
        System.IO.Stream ProductCodes();

        [WebInvoke(Method = "GET", UriTemplate = "productcodes/{inc}")]
        System.IO.Stream ProductCodesInc(string inc);
    }
}