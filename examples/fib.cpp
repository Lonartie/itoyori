#include <ityr/ityr.hpp>

static volatile uint64_t result;

uint64_t fib(uint64_t n);

static std::size_t min_frame_size = std::numeric_limits<std::size_t>::max();
static std::size_t max_frame_size = std::numeric_limits<std::size_t>::min();

int main(int argc, char** argv) {
   ityr::init();
   ityr::profiler_begin();

   const int iterations = argc > 1 ? std::stoi(argv[1]) : 10;
   const int input = argc > 2 ? std::stoi(argv[2]) : 20;

   ityr::root_exec([=]() -> void {
      for (int i = 0; i < iterations; i++) {
         result = fib(input);
         std::cout << "iteration #" << i + 1 << " done\n";
      }
   });

   ityr::profiler_end();
   ityr::profiler_flush();

   ityr::root_exec([=]() -> void {
      ityr::global_vector<std::size_t> get(ityr::n_ranks()), put(ityr::n_ranks()), cas(ityr::n_ranks()),
            faa(ityr::n_ranks()), faog(ityr::n_ranks()), faop(ityr::n_ranks()), send(ityr::n_ranks()),
            recv(ityr::n_ranks()), brdc(ityr::n_ranks()), rcvc(ityr::n_ranks()), sendc(ityr::n_ranks());
      ityr::global_span<std::size_t> sget(get), sput(put), scas(cas), sfaa(faa), sfaog(faog), sfaop(faop), ssend(send),
            srecv(recv), sbrdc(brdc), srcvc(rcvc), ssendc(sendc);

      ityr::global_vector<std::size_t> stolen_count(ityr::n_ranks()), stolen_size(ityr::n_ranks());
      ityr::global_span<std::size_t> s_stolen_count(stolen_count), s_stolen_size(stolen_size);

      ityr::coll_exec([=]() {
         auto c_stolen_size = ityr::make_checkout(s_stolen_size, ityr::checkout_mode::read_write);
         c_stolen_size[ityr::my_rank()] = ityr::ito::STOLEN_FRAMES_SIZE;

         auto c_stolen_count = ityr::make_checkout(s_stolen_count, ityr::checkout_mode::read_write);
         c_stolen_count[ityr::my_rank()] = ityr::ito::STOLEN_FRAMES_COUNT;

         auto cget = ityr::make_checkout(sget, ityr::checkout_mode::read_write);
         cget[ityr::my_rank()] = ityr::common::RMA_GET_DATA_SIZE;

         auto cput = ityr::make_checkout(sput, ityr::checkout_mode::read_write);
         cput[ityr::my_rank()] = ityr::common::RMA_PUT_DATA_SIZE;

         auto ccas = ityr::make_checkout(scas, ityr::checkout_mode::read_write);
         ccas[ityr::my_rank()] = ityr::common::RMA_CAS_DATA_SIZE;

         auto cfaa = ityr::make_checkout(sfaa, ityr::checkout_mode::read_write);
         cfaa[ityr::my_rank()] = ityr::common::RMA_FAA_DATA_SIZE;

         auto cfaog = ityr::make_checkout(sfaog, ityr::checkout_mode::read_write);
         cfaog[ityr::my_rank()] = ityr::common::RMA_FAO_GET_DATA_SIZE;

         auto cfaop = ityr::make_checkout(sfaop, ityr::checkout_mode::read_write);
         cfaop[ityr::my_rank()] = ityr::common::RMA_FAO_PUT_DATA_SIZE;

         auto csend = ityr::make_checkout(ssend, ityr::checkout_mode::read_write);
         csend[ityr::my_rank()] = ityr::common::MPI_SEND_SIZE;

         auto crecv = ityr::make_checkout(srecv, ityr::checkout_mode::read_write);
         crecv[ityr::my_rank()] = ityr::common::MPI_RECV_SIZE;

         auto cbrdc = ityr::make_checkout(sbrdc, ityr::checkout_mode::read_write);
         cbrdc[ityr::my_rank()] = ityr::common::MPI_BROADCAST_SIZE;

         auto crecvc = ityr::make_checkout(srcvc, ityr::checkout_mode::read_write);
         crecvc[ityr::my_rank()] = ityr::common::MPI_RECV_SIZE;

         auto csendc = ityr::make_checkout(ssendc, ityr::checkout_mode::read_write);
         csendc[ityr::my_rank()] = ityr::common::MPI_BROADCAST_SIZE;
      });

      {
         auto size = ityr::make_checkout(s_stolen_size, ityr::checkout_mode::read_write);
         auto count = ityr::make_checkout(s_stolen_count, ityr::checkout_mode::read_write);
         std::size_t gsize = 0, gcount = 0;
         for (int i = 0; i < ityr::n_ranks(); i++) {
            gsize += size[i];
            gcount += count[i];
         }
         printf("stolen %llu (%llu bytes total, %f bytes avg)", gcount, gsize, (float)gsize / (float)gcount);
      }

      {
         auto cget = ityr::make_checkout(sget, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cget) {
            sum += v;
         }
         printf("get: %llu\n", sum);
      }

      {
         auto cput = ityr::make_checkout(sput, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cput) {
            sum += v;
         }
         printf("put: %llu\n", sum);
      }

      {
         auto ccas = ityr::make_checkout(scas, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: ccas) {
            sum += v;
         }
         printf("cas: %llu\n", sum);
      }

      {
         auto cfaa = ityr::make_checkout(sfaa, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cfaa) {
            sum += v;
         }
         printf("faa: %llu\n", sum);
      }

      {
         auto cfaog = ityr::make_checkout(sfaog, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cfaog) {
            sum += v;
         }
         printf("faog: %llu\n", sum);
      }

      {
         auto cfaop = ityr::make_checkout(sfaop, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cfaop) {
            sum += v;
         }
         printf("faop: %llu\n", sum);
      }

      {
         auto csend = ityr::make_checkout(ssend, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: csend) {
            sum += v;
         }
         printf("send: %llu\n", sum);
      }

      {
         auto crecv = ityr::make_checkout(srecv, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: crecv) {
            sum += v;
         }
         printf("recv: %llu\n", sum);
      }

      {
         auto cbrdc = ityr::make_checkout(sbrdc, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: cbrdc) {
            sum += v;
         }
         printf("brdc: %llu\n", sum);
      }

      {
         auto csendc = ityr::make_checkout(ssendc, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: csendc) {
            sum += v;
         }
         printf("send#: %llu\n", sum);
      }

      {
         auto crecvc = ityr::make_checkout(srcvc, ityr::checkout_mode::read_write);
         uint64_t sum = 0;
         for (auto& v: crecvc) {
            sum += v;
         }
         printf("recv#: %llu\n", sum);
      }
   });
   ityr::fini();
}

uint64_t fib(const uint64_t n) {
   // printf("fib fs: %llu\n", ityr::ito::CURRENT_CONTEXT_FRAME_SIZE);

   if (n <= 2)
      return 1;

   const auto [a, b] = ityr::parallel_invoke([=] { return fib(n - 1); }, [=] { return fib(n - 2); });

   return a + b;
}
