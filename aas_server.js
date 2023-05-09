const { OPCUAServer, DataType, Variant, VariantArrayType, StatusCodes, makeAccessLevelFlag } = require("node-opcua");
var ModbusRTU = require("modbus-serial");
var modbusClient = new ModbusRTU();

var turning_ip = "192.168.0.120";
var robot_ip = "192.168.0.121";
var asrs_ip = "192.168.0.122";
let i = 0;
(async () => {

    try {

        const server = new OPCUAServer({
            port: 4334 // the port of the listening socket of the server
        });

        await server.initialize();

        const addressSpace = server.engine.addressSpace;
        const namespace = addressSpace.getOwnNamespace();

        //create the object, each station is a object in the server
        const fms = namespace.addObject({
            organizedBy: addressSpace.rootFolder.objects,
            browseName: "FMS_CAP"
        });

        const turningStation = namespace.addObject({
            organizedBy: addressSpace.rootFolder.objects,
            browseName: "Turning Station"
        });

        //for each station, the methods and variables are create
        const method = namespace.addMethod(turningStation, {
            browseName: "Run",

            inputArguments: [
                {
                    name: "partID",
                    description: { text: "ID of the gcode file to run" },
                    dataType: DataType.UInt32
                }
            ],

            outputArguments: [
                {
                    name: "ack",
                    description: { text: "Acknowledge" },
                    dataType: DataType.String,
                    valueRank: 1
                }
            ]
        });

        // optionally, we can adjust userAccessLevel attribute
        method.outputArguments.userAccessLevel = makeAccessLevelFlag("CurrentRead");
        method.inputArguments.userAccessLevel = makeAccessLevelFlag("CurrentRead");

        method.bindMethod((inputArguments, context) => {
            const id = inputArguments[0].value;
            //const volume = inputArguments[1].value;
            console.log("sending request to turning station");

            const callMethodResult = {
                statusCode: StatusCodes.Good,
                outputArguments: [
                    {
                        dataType: DataType.String,
                        arrayType: VariantArrayType.Array,
                        value: "runing"
                    }
                ]
            };
            return callMethodResult;
        });



        namespace.addVariable({
            componentOf: turningStation,
            nodeId: "s=turning_status", // a string nodeID
            browseName: "Status",
            dataType: "Double",
            value: {
                get: () => new Variant({ dataType: DataType.Double, value: read_status(turning_ip) })
            }
        });


        await server.start();
        console.log("Server is now listening ... ( press CTRL+C to stop)");
        const endpointUrl = server.endpoints[0].endpointDescriptions()[0].endpointUrl;
        console.log(" the primary server endpoint url is ", endpointUrl);
    } catch (err) {
        console.log(err);
    }
})();