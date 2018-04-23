import json
import logging

from hash_ring import HashRing

from nose.plugins.base import Plugin
from nose.util import test_address

logger = logging.getLogger('nose.plugins.distributed_nose')


class DistributedNose(Plugin):
    """
    Distribute a test run, shared-nothing style, by specifying the total number
    of runners and a unique ID for this runner.
    """
    name = 'distributed'

    ALGORITHM_HASH_RING = 0
    ALGORITHM_LEAST_PROCESSING_TIME = 1
    algorithms = {
        "hash-ring": ALGORITHM_HASH_RING,
        "least-processing-time": ALGORITHM_LEAST_PROCESSING_TIME
    }

    def __init__(self):
        Plugin.__init__(self)

        self.node_count = None
        self.node_id = None
        self.hash_ring = None
        self.lpt_nodes = None
        self.lpt_data = None

    def options(self, parser, env):
        parser.add_option(
            "--nodes",
            action="store",
            dest="distributed_nodes",
            default=env.get('NOSE_NODES', 1),
            metavar="DISTRIBUTED_NODES",
            help="Across how many nodes are tests being distributed?",
        )
        parser.add_option(
            "--node-number",
            action="store",
            dest="distributed_node_number",
            default=env.get('NOSE_NODE_NUMBER', 1),
            metavar="DISTRIBUTED_NODE_NUMBER",
            help=(
                "Of the total nodes running distributed tests, "
                "which number is this node? (1-indexed)"
            ),
        )
        parser.add_option(
            "--distributed-disabled",
            action="store_true",
            dest="distributed_disabled",
            default=False,
            metavar="DISTRIBUTED_DISABLED",
            help=((
                "Set this flag to disable distribution, "
                "despite having more than 1 node configured. "
                "This is useful if you use environment configs "
                "and want to temporarily disable test distribution."
            )),
        )
        parser.add_option(
            "--hash-by-class",
            action="store_true",
            dest="distributed_hash_by_class",
            default=bool(env.get('NOSE_HASH_BY_CLASS', False)),  # any non-empty value enables
            metavar="DISTRIBUTED_HASH_BY_CLASS",
            help=((
                "By default, tests are distributed individually. "
                "This results in the most even distribution and the"
                " best speed if all tests have the same runtime. "
                "However, it duplicates class setup/teardown work; "
                "set this flag to keep tests in the same class on the same node. "
            )),
        )
        parser.add_option(
            "--algorithm",
            action="store",
            dest="algorithm",
            choices=(
                "hash-ring",
                "least-processing-time"
            ),
            default=env.get('NOSE_DISTRIBUTION_ALGORITHM', "hash-ring"),
            metavar="ALGORITHM",
            help=(
                "Specify an algorithm [hash-ring|least-processing-time] "
                "to use to distribute the tests. By default, tests are "
                "distributed using a hash ring. If least-processing-time "
                "is specified, you must also provide a filepath for the "
                "duration data with the --lpt-data option."
            ),
        )
        parser.add_option(
            "--lpt-data",
            action="store",
            dest="lpt_data_filepath",
            default=env.get('NOSE_LPT_DATA_FILEPATH'),
            help=(
                "The filepath from which to retrieve the data to use for "
                "the least processing time algorithm. Required when "
                "using '--algorithm=least-processing-time'."
            ),
        )

    def configure(self, options, config):
        self.node_count = options.distributed_nodes
        self.node_id = options.distributed_node_number
        self.hash_by_class = options.distributed_hash_by_class
        self.algorithm = self.algorithms[options.algorithm]
        self.lpt_data_filepath = options.lpt_data_filepath

        if not self._options_are_valid():
            self.enabled = False
            return

        if options.distributed_disabled:
            self.enabled = False
            return

        if self.node_count > 1:
            # If the user gives us a non-1 count of distributed nodes, then
            # let's distribute their tests
            self.enabled = True

        if self.algorithm == self.ALGORITHM_LEAST_PROCESSING_TIME:
            assert self.lpt_data_filepath, "'--lpt-data' arg is set."

            try:
                # Set up the data structure for the nodes. Note that
                # the 0th node is a dummy node. We do this since the nodes
                # are 1-indexed and this prevents the need to do
                # offsetting when we access this structure with
                # a node-number elsewhere.

                with open(self.lpt_data_filepath) as f:

                    self.lpt_data = json.load(f)
                    self.lpt_nodes = [
                        {
                            'processing_time': 0,
                            'objects': set()
                        }
                        for _ in range(self.node_count + 1)
                    ]

                    sorted_lpt_data = sorted(
                        self.lpt_data.items(),
                        key=lambda t: t[1]['duration'],
                        reverse=True
                    )

                    for obj, data in sorted_lpt_data:
                        node = min(
                            self.lpt_nodes[1:],
                            key=lambda n: n['processing_time']
                        )
                        node['processing_time'] += data['duration']
                        node['objects'].add(obj)

            except IOError:
                logger.critical(
                    "lpt-data file '%s' not found. Aborting.",
                    self.lpt_data_filepath
                )
                raise
            except ValueError:
                logger.critical(
                    "Error decoding lpt-data file. Aborting."
                )
                raise
            except KeyError:
                logger.critical(
                    "Invalid lpt data file. Aborting."
                )
                raise

        self.hash_ring = HashRing(range(1, self.node_count + 1))

    def _options_are_valid(self):
        try:
            self.node_count = int(self.node_count)
        except ValueError:
            logger.critical("--nodes must be an integer")
            return False

        try:
            self.node_id = int(self.node_id)
        except ValueError:
            logger.critical("--node-number must be an integer")
            return False

        if self.node_id > self.node_count:
            logger.critical((
                "--node-number can't be larger "
                "than the number of nodes"
            ))
            return False

        if self.node_id < 1:
            logger.critical(
                "--node-number must be greater than zero"
            )
            return False

        return True

    def validateName(self, testObject):
        try:
            _, module, call = test_address(testObject)
        except TypeError:
            module = 'unknown'
            call = str(testObject)

        node = self.hash_ring.get_node('%s.%s' % (module, call))
        if node != self.node_id:
            return False

        return None

    def wantClass(self, cls):
        if not self.hash_by_class:
            # Defer to wantMethod.
            return None

        if self.algorithm == self.ALGORITHM_HASH_RING:
            node = self.hash_ring.get_node(str(cls))
            if node != self.node_id:
                return False
        elif self.algorithm == self.ALGORITHM_LEAST_PROCESSING_TIME and self.hash_by_class:
            namespaced_class = '{}.{}'.format(
                cls.__module__,
                cls.__name__
            )
            if namespaced_class in self.lpt_data:
                return namespaced_class in self.lpt_nodes[self.node_id]['objects']
            else:
                # When we don't have duration data for this class,
                # use the hash ring to get a node. This seems safer
                # than trying to dynamically update the distribution
                # using LPT, since it is guaranteed to be deterministic
                # across nodes (whereas dynamic LPT would be deterministic
                # only if we were given the classes for consideration in
                # the same order across all nodes).
                node = self.hash_ring.get_node(str(cls))
                if node != self.node_id:
                    return False

        return None

    def wantMethod(self, method):
        if self.hash_by_class:
            return None
        if self.algorithm == self.ALGORITHM_LEAST_PROCESSING_TIME and not self.hash_by_class:
            full_name = "{}.{}.{}".format(method.__module__, method.im_class.__name__, method.__name__)
            if full_name in self.lpt_data:
                return full_name in self.lpt_nodes[self.node_id]['objects']
            else:
                node = self.hash_ring.get_node(full_name)
                if node != self.node_id:
                    return False
        return self.validateName(method)

    def wantFunction(self, function):
        # Always operate directly on bare functions.
        return self.validateName(function)
