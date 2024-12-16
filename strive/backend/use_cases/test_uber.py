import time
from unittest import TestCase

from core.system_processor import get_cycles_and_dag_paths, verify_cycles
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def test_monolith(self):
        print(time.process_time())

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers", [Column("driver_id", int), Column("location_near", str), Column("available", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str), OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx["customer_current_location"]),
                                       ["current_location"])

        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        UpdateDriverToBusy = Operation("UpdateDriverToBusy",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        RideRequest = LocalTransaction([getCustomerLocation, getAvailableDriver, insertNewRide, UpdateDriverToBusy], [InputParameter("customer_id", int), InputParameter("desired_destination", str)])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RideRequest],  [InputParameter("customer_id", int), InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                       [InputParameter("ride_id", int)],
                                       OperationType.WRITE,
                                       Rides,
                                       lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                       ["status"])

        getRideDetails = Operation("getRideDetails",
                                        [InputParameter("ride_id", int),
                                         OutputParameter("driver_id", int),
                                         OutputParameter("destination", str),
                                         OutputParameter("customer_id", int)],
                                        OperationType.READ,
                                        Rides,
                                        lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                        ["driver_id", "destination"])

        UpdateDriverToAvailable = Operation("UpdateDriverToAvailable",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                  [InputParameter("ride_id", int),
                                   InputParameter("destination", str),
                                   InputParameter("customer_id", int)],
                                  OperationType.WRITE,
                                  Customers,
                                  lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                  ["last_ride_id", "current_location"])

        RideCompletion = LocalTransaction([CompleteRide, getRideDetails, UpdateDriverToAvailable, UpdateCustomerHistory],
                                       [InputParameter("ride_id", int)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [RideCompletion], [InputParameter("ride_id", int)])

        ms = Microservice("ms", [RideRequest_BT, RideCompletion_BT])

        system = System([ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)

        verify_cycles(topological_paths_for_cycles, system)

    def test_micro(self):

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers", [Column("driver_id", int), Column("location_near", str), Column("available", str), Column("status", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getCustomerLocation_BT = BusinessTransaction("getCustomerLocation_BT",
                                                     [LocalTransaction([getCustomerLocation], [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)])],
                                                     [InputParameter("customer_id", int)])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str), OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx["customer_current_location"]),
                                       ["current_location"])

        getAvailableDriver_BT = BusinessTransaction("getAvailableDriver_BT", [LocalTransaction([getAvailableDriver], [OutputParameter("driver_id", int)])], [])


        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        UpdateDriverToBusy = Operation("UpdateDriverToBusy",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        UpdateDriverToBusy_BT = BusinessTransaction("UpdateDriverToBusy_BT", [LocalTransaction([UpdateDriverToBusy], [])], [InputParameter("driver_id", int)])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RemoteBusinessTransaction(getCustomerLocation_BT),
                                                                RemoteBusinessTransaction(getAvailableDriver_BT),
                                                                LocalTransaction([insertNewRide], []),
                                                                RemoteBusinessTransaction(UpdateDriverToBusy_BT)],
                                             [InputParameter("customer_id", int),
                                              InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                       [InputParameter("ride_id", int)],
                                       OperationType.WRITE,
                                       Rides,
                                       lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                       ["status"])

        getRideDetails = Operation("getRideDetails",
                                        [InputParameter("ride_id", int),
                                         OutputParameter("driver_id", int),
                                         OutputParameter("destination", str),
                                         OutputParameter("customer_id", int)],
                                        OperationType.READ,
                                        Rides,
                                        lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                        ["driver_id", "destination"])

        UpdateDriverToAvailable = Operation("UpdateDriverToAvailable",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        UpdateDriverToAvailable_BT = BusinessTransaction("UpdateDriverToAvailable_BT", [LocalTransaction([UpdateDriverToAvailable], [])], [InputParameter("driver_id", int)])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                  [InputParameter("ride_id", int),
                                   InputParameter("destination", str),
                                   InputParameter("customer_id", int)],
                                  OperationType.WRITE,
                                  Customers,
                                  lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                  ["last_ride_id", "current_location"])

        UpdateCustomerHistory_BT = BusinessTransaction("UpdateCustomerHistory_BT",
                                                       [LocalTransaction([UpdateCustomerHistory], [])],
                                                       [InputParameter("customer_id", int),
                                                        InputParameter("ride_id", int),
                                                        InputParameter("destination", str)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [LocalTransaction([CompleteRide, getRideDetails], [OutputParameter("customer_id", int), OutputParameter("destination", str)]),
                                                                      RemoteBusinessTransaction(UpdateDriverToAvailable_BT),
                                                                      RemoteBusinessTransaction(UpdateCustomerHistory_BT)],
                                                [InputParameter("ride_id", int), InputParameter("driver_id", int)])

        ride_ms = Microservice("RIDE", [RideRequest_BT, RideCompletion_BT])
        customer_ms = Microservice("CUSTOMER", [getCustomerLocation_BT, UpdateCustomerHistory_BT])
        driver_ms = Microservice("DRIVER", [getAvailableDriver_BT, UpdateDriverToAvailable_BT, UpdateDriverToBusy_BT])

        system = System([ride_ms, customer_ms, driver_ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)

        for c in cycles:
            print(c)

        #for p in paths:
        #    print(p)

        verify_cycles(topological_paths_for_cycles, system)

    def test_micro_isolated_non_acid(self):

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers", [Column("driver_id", int), Column("location_near", str), Column("available", str), Column("status", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getCustomerLocation_BT = InternalBusinessTransaction("getCustomerLocation_BT",
                                                     [LocalTransaction([getCustomerLocation], [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)])],
                                                     [InputParameter("customer_id", int)])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str), OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx["customer_current_location"]),
                                       ["current_location"])

        getAvailableDriver_BT = InternalBusinessTransaction("getAvailableDriver_BT", [LocalTransaction([getAvailableDriver], [OutputParameter("driver_id", int)])], [])


        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        UpdateDriverToBusy = Operation("UpdateDriverToBusy",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        UpdateDriverToBusy_BT = InternalBusinessTransaction("UpdateDriverToBusy_BT", [LocalTransaction([UpdateDriverToBusy], [])], [InputParameter("driver_id", int)])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RemoteBusinessTransaction(getCustomerLocation_BT),
                                                                RemoteBusinessTransaction(getAvailableDriver_BT),
                                                                LocalTransaction([insertNewRide], []),
                                                                RemoteBusinessTransaction(UpdateDriverToBusy_BT)],
                                             [InputParameter("customer_id", int),
                                              InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                       [InputParameter("ride_id", int)],
                                       OperationType.WRITE,
                                       Rides,
                                       lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                       ["status"])

        getRideDetails = Operation("getRideDetails",
                                        [InputParameter("ride_id", int),
                                         OutputParameter("driver_id", int),
                                         OutputParameter("destination", str),
                                         OutputParameter("customer_id", int)],
                                        OperationType.READ,
                                        Rides,
                                        lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                        ["driver_id", "destination"])

        UpdateDriverToAvailable = Operation("UpdateDriverToAvailable",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                  ["status"])

        UpdateDriverToAvailable_BT = InternalBusinessTransaction("UpdateDriverToAvailable_BT", [LocalTransaction([UpdateDriverToAvailable], [])], [InputParameter("driver_id", int)])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                  [InputParameter("ride_id", int),
                                   InputParameter("destination", str),
                                   InputParameter("customer_id", int)],
                                  OperationType.WRITE,
                                  Customers,
                                  lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                  ["last_ride_id", "current_location"])

        UpdateCustomerHistory_BT = InternalBusinessTransaction("UpdateCustomerHistory_BT",
                                                       [LocalTransaction([UpdateCustomerHistory], [])],
                                                       [InputParameter("customer_id", int),
                                                        InputParameter("ride_id", int),
                                                        InputParameter("destination", str)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [LocalTransaction([CompleteRide],
                                                                                       []),
                                                                      LocalTransaction([getRideDetails],
                                                                                       [OutputParameter("customer_id", int),
                                                                                        OutputParameter("destination", str)]),
                                                                      RemoteBusinessTransaction(UpdateDriverToAvailable_BT),
                                                                      RemoteBusinessTransaction(UpdateCustomerHistory_BT)],
                                                [InputParameter("ride_id", int), InputParameter("driver_id", int)])

        ride_ms = Microservice("RIDE", [RideRequest_BT, RideCompletion_BT])
        customer_ms = Microservice("CUSTOMER", [getCustomerLocation_BT, UpdateCustomerHistory_BT])
        driver_ms = Microservice("DRIVER", [getAvailableDriver_BT, UpdateDriverToAvailable_BT, UpdateDriverToBusy_BT])

        system = System([ride_ms, customer_ms, driver_ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)

        #for c in cycles:
        #    print(c)

        #for p in paths:
        #    print(p)

        verify_cycles(topological_paths_for_cycles, system)


    def test_micro_isolated(self):

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers", [Column("driver_id", int), Column("location_near", str), Column("available", str), Column("status", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getCustomerLocation_BT = InternalBusinessTransaction("getCustomerLocation_BT",
                                                     [LocalTransaction([getCustomerLocation], [InputParameter("customer_id", int), OutputParameter("customer_current_location", str)])],
                                                     [InputParameter("customer_id", int)])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str), OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx["customer_current_location"]),
                                       ["driver_id", "location_near", "available", "status"])

        getAvailableDriver_BT = InternalBusinessTransaction("getAvailableDriver_BT", [LocalTransaction([getAvailableDriver], [OutputParameter("driver_id", int)])], [])


        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        UpdateDriverToBusy = Operation("UpdateDriverToBusy",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: z3.And(Drivers.column("driver_id") == ctx["driver_id"],
                                                     Drivers.column("status") != "busy"),
                                  ["status"])

        UpdateDriverToBusy_BT = InternalBusinessTransaction("UpdateDriverToBusy_BT", [LocalTransaction([UpdateDriverToBusy], [])], [InputParameter("driver_id", int)])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RemoteBusinessTransaction(getCustomerLocation_BT),
                                                                RemoteBusinessTransaction(getAvailableDriver_BT),
                                                                LocalTransaction([insertNewRide], []),
                                                                RemoteBusinessTransaction(UpdateDriverToBusy_BT)],
                                             [InputParameter("customer_id", int),
                                              InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                       [InputParameter("ride_id", int)],
                                       OperationType.WRITE,
                                       Rides,
                                       lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                       ["status"])

        # output driver id? cria mais anomalias
        getRideDetails = Operation("getRideDetails",
                                        [InputParameter("ride_id", int),
                                         OutputParameter("destination", str),
                                         OutputParameter("customer_id", int)],
                                        OperationType.READ,
                                        Rides,
                                        lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                        ["driver_id", "destination"])

        UpdateDriverToAvailable = Operation("UpdateDriverToAvailable",
                                  [InputParameter("driver_id", int)],
                                  OperationType.WRITE,
                                  Drivers,
                                  lambda ctx: z3.And(Drivers.column("driver_id") == ctx["driver_id"],
                                                     Drivers.column("status") != "available"),
                                  ["status"])

        UpdateDriverToAvailable_BT = InternalBusinessTransaction("UpdateDriverToAvailable_BT", [LocalTransaction([UpdateDriverToAvailable], [])], [InputParameter("driver_id", int)])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                  [InputParameter("ride_id", int),
                                   InputParameter("destination", str),
                                   InputParameter("customer_id", int)],
                                  OperationType.WRITE,
                                  Customers,
                                  lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                  ["last_ride_id", "current_location"])

        UpdateCustomerHistory_BT = InternalBusinessTransaction("UpdateCustomerHistory_BT",
                                                       [LocalTransaction([UpdateCustomerHistory], [])],
                                                       [InputParameter("customer_id", int),
                                                        InputParameter("ride_id", int),
                                                        InputParameter("destination", str)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [LocalTransaction([CompleteRide, getRideDetails], [OutputParameter("customer_id", int), OutputParameter("destination", str)]),
                                                                      RemoteBusinessTransaction(UpdateDriverToAvailable_BT),
                                                                      RemoteBusinessTransaction(UpdateCustomerHistory_BT)],
                                                [InputParameter("ride_id", int), InputParameter("driver_id", int)])

        ride_ms = Microservice("RIDE", [RideRequest_BT, RideCompletion_BT])
        customer_ms = Microservice("CUSTOMER", [getCustomerLocation_BT, UpdateCustomerHistory_BT])
        driver_ms = Microservice("DRIVER", [getAvailableDriver_BT, UpdateDriverToAvailable_BT, UpdateDriverToBusy_BT])

        system = System([ride_ms, customer_ms, driver_ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)

        #for c in cycles:
        #    print(c)

        #for p in paths:
        #   print(p)

        verify_cycles(topological_paths_for_cycles, system)

    def test_micro_isolated_with_update_status_vacation_ex(self):

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers",
                        [Column("driver_id", int), Column("location_near", str), Column("available", str),
                         Column("status", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int),
                                         OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getCustomerLocation_BT = InternalBusinessTransaction("getCustomerLocation_BT",
                                                             [LocalTransaction([getCustomerLocation],
                                                                               [InputParameter("customer_id", int),
                                                                                OutputParameter(
                                                                                    "customer_current_location",
                                                                                    str)])],
                                                             [InputParameter("customer_id", int)])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str),
                                        OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx[
                                                              "customer_current_location"]),
                                       ["driver_id", "location_near", "available", "status"])

        getAvailableDriver_BT = InternalBusinessTransaction("getAvailableDriver_BT", [
            LocalTransaction([getAvailableDriver], [OutputParameter("driver_id", int)])], [])

        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        UpdateDriverToBusy = Operation("UpdateDriverToBusy",
                                       [InputParameter("driver_id", int)],
                                       OperationType.WRITE,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("driver_id") == ctx["driver_id"],
                                                          Drivers.column("status") != "busy"),
                                       ["status"])

        UpdateDriverToBusy_BT = InternalBusinessTransaction("UpdateDriverToBusy_BT",
                                                            [LocalTransaction([UpdateDriverToBusy], [])],
                                                            [InputParameter("driver_id", int)])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RemoteBusinessTransaction(getCustomerLocation_BT),
                                                                RemoteBusinessTransaction(getAvailableDriver_BT),
                                                                LocalTransaction([insertNewRide], []),
                                                                RemoteBusinessTransaction(UpdateDriverToBusy_BT)],
                                             [InputParameter("customer_id", int),
                                              InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                 [InputParameter("ride_id", int)],
                                 OperationType.WRITE,
                                 Rides,
                                 lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                 ["status"])

        # output driver id? cria mais anomalias
        getRideDetails = Operation("getRideDetails",
                                   [InputParameter("ride_id", int),
                                    OutputParameter("destination", str),
                                    OutputParameter("customer_id", int)],
                                   OperationType.READ,
                                   Rides,
                                   lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                   ["driver_id", "destination"])

        UpdateDriverToAvailable = Operation("UpdateDriverToAvailable",
                                            [InputParameter("driver_id", int)],
                                            OperationType.WRITE,
                                            Drivers,
                                            lambda ctx: z3.And(Drivers.column("driver_id") == ctx["driver_id"],
                                                               Drivers.column("status") != "available"),
                                            ["status"])

        UpdateDriverToAvailable_BT = InternalBusinessTransaction("UpdateDriverToAvailable_BT",
                                                                 [LocalTransaction([UpdateDriverToAvailable], [])],
                                                                 [InputParameter("driver_id", int)])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                          [InputParameter("ride_id", int),
                                           InputParameter("destination", str),
                                           InputParameter("customer_id", int)],
                                          OperationType.WRITE,
                                          Customers,
                                          lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                          ["last_ride_id", "current_location"])

        UpdateCustomerHistory_BT = InternalBusinessTransaction("UpdateCustomerHistory_BT",
                                                               [LocalTransaction([UpdateCustomerHistory], [])],
                                                               [InputParameter("customer_id", int),
                                                                InputParameter("ride_id", int),
                                                                InputParameter("destination", str)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [
            LocalTransaction([CompleteRide, getRideDetails],
                             [OutputParameter("customer_id", int), OutputParameter("destination", str)]),
            RemoteBusinessTransaction(UpdateDriverToAvailable_BT),
            RemoteBusinessTransaction(UpdateCustomerHistory_BT)],
                                                [InputParameter("ride_id", int), InputParameter("driver_id", int)])

        UpdateDriverOff = Operation("UpdateDriver",
                                    [InputParameter("driver_id", int)],
                                    OperationType.WRITE,
                                    Drivers,
                                    lambda ctx: z3.And(Drivers.column("driver_id") == ctx["driver_id"],
                                                       Drivers.column("status") != "busy"),
                                    ["status"])

        UpdateDriver_BT = BusinessTransaction("UpdateDriver_BT",
                                              [LocalTransaction([UpdateDriverOff], [])],
                                              [InputParameter("driver_id", int)])

        ride_ms = Microservice("RIDE", [RideRequest_BT, RideCompletion_BT])
        customer_ms = Microservice("CUSTOMER", [getCustomerLocation_BT, UpdateCustomerHistory_BT])
        driver_ms = Microservice("DRIVER",
                                 [getAvailableDriver_BT, UpdateDriverToAvailable_BT, UpdateDriverToBusy_BT, UpdateDriver_BT])

        system = System([ride_ms, customer_ms, driver_ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, cycle_assertions = get_cycles_and_dag_paths(graph)

        #for c in cycle_assertions:
        #    print(c)
        #    print("\t", cycle_assertions[c])

        #print("paths len", len(paths))
        #for p in paths:
        #    print(p)

        verify_cycles(topological_paths_for_cycles, system)


    def test_micro_isolated_with_generic_update_status_vacation_ex(self):

        Customers = Table("Customers", [Column("customer_id", int), Column("current_location", str)])

        Drivers = Table("Drivers",
                        [Column("driver_id", int), Column("location_near", str), Column("available", str),
                         Column("status", str)])

        Rides = Table("Rides", [Column("customer_id", int),
                                Column("driver_id", int),
                                Column("start_location", str),
                                Column("destination", str),
                                Column("status", str),
                                Column("ride_id", int)])


        UpdateDriver = Operation("UpdateDriver",
                                            [InputParameter("driver_id", int)],
                                            OperationType.WRITE,
                                            Drivers,
                                            lambda ctx: Drivers.column("driver_id") == ctx["driver_id"],
                                            ["status"])

        UpdateDriver_BT = BusinessTransaction("UpdateDriver_BT",
                                              [LocalTransaction([UpdateDriver], [])],
                                              [InputParameter("driver_id", int)])

        getCustomerLocation = Operation("getCustomerLocation",
                                        [InputParameter("customer_id", int),
                                         OutputParameter("customer_current_location", str)],
                                        OperationType.READ,
                                        Customers,
                                        lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                        ["current_location"])

        getCustomerLocation_BT = InternalBusinessTransaction("getCustomerLocation_BT",
                                                             [LocalTransaction([getCustomerLocation],
                                                                               [InputParameter("customer_id", int),
                                                                                OutputParameter(
                                                                                    "customer_current_location",
                                                                                    str)])],
                                                             [InputParameter("customer_id", int)])

        getAvailableDriver = Operation("getAvailableDriver",
                                       [InputParameter("customer_current_location", str),
                                        OutputParameter("driver_id", int)],
                                       OperationType.READ,
                                       Drivers,
                                       lambda ctx: z3.And(Drivers.column("status") == "available",
                                                          Drivers.column("location_near") == ctx[
                                                              "customer_current_location"]),
                                       ["driver_id", "location_near", "available", "status"])

        getAvailableDriver_BT = InternalBusinessTransaction("getAvailableDriver_BT", [
            LocalTransaction([getAvailableDriver], [OutputParameter("driver_id", int)])], [])

        insertNewRide = Operation("insertNewRide",
                                  [InputParameter("customer_id", int),
                                   InputParameter("driver_id", int),
                                   InputParameter("customer_current_location", str),
                                   InputParameter("desired_destination", str)],
                                  OperationType.WRITE,
                                  Rides,
                                  [],
                                  ["customer_id", "driver_id", "start_location", "destination", "status"])

        RideRequest_BT = BusinessTransaction("RideRequest_BT", [RemoteBusinessTransaction(getCustomerLocation_BT),
                                                                RemoteBusinessTransaction(getAvailableDriver_BT),
                                                                LocalTransaction([insertNewRide], []),
                                                                RemoteBusinessTransaction(UpdateDriver_BT)],
                                             [InputParameter("customer_id", int),
                                              InputParameter("desired_destination", str)])

        CompleteRide = Operation("CompleteRide",
                                 [InputParameter("ride_id", int)],
                                 OperationType.WRITE,
                                 Rides,
                                 lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                 ["status"])

        # output driver id? cria mais anomalias
        getRideDetails = Operation("getRideDetails",
                                   [InputParameter("ride_id", int),
                                    OutputParameter("destination", str),
                                    OutputParameter("customer_id", int)],
                                   OperationType.READ,
                                   Rides,
                                   lambda ctx: Rides.column("ride_id") == ctx["ride_id"],
                                   ["driver_id", "destination"])

        UpdateCustomerHistory = Operation("UpdateCustomerHistory",
                                          [InputParameter("ride_id", int),
                                           InputParameter("destination", str),
                                           InputParameter("customer_id", int)],
                                          OperationType.WRITE,
                                          Customers,
                                          lambda ctx: Customers.column("customer_id") == ctx["customer_id"],
                                          ["last_ride_id", "current_location"])

        UpdateCustomerHistory_BT = InternalBusinessTransaction("UpdateCustomerHistory_BT",
                                                               [LocalTransaction([UpdateCustomerHistory], [])],
                                                               [InputParameter("customer_id", int),
                                                                InputParameter("ride_id", int),
                                                                InputParameter("destination", str)])

        RideCompletion_BT = BusinessTransaction("RideCompletion_BT", [
            LocalTransaction([CompleteRide, getRideDetails],
                             [OutputParameter("customer_id", int), OutputParameter("destination", str)]),
            RemoteBusinessTransaction(UpdateDriver_BT),
            RemoteBusinessTransaction(UpdateCustomerHistory_BT)],
                                                [InputParameter("ride_id", int), InputParameter("driver_id", int)])

        ride_ms = Microservice("RIDE", [RideRequest_BT, RideCompletion_BT])
        customer_ms = Microservice("CUSTOMER", [getCustomerLocation_BT, UpdateCustomerHistory_BT])
        driver_ms = Microservice("DRIVER",
                                 [getAvailableDriver_BT, UpdateDriver_BT])

        system = System([ride_ms, customer_ms, driver_ms], [])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)

        #for c in cycles:
        #    print(c)

        #for p in paths:
        #    print(p)

        verify_cycles(topological_paths_for_cycles, system)