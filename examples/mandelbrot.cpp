#include <iostream>
#include <ityr/ityr.hpp>

int mandelbrot(double real, double imag, int max_iter) {
   double z_real = 0.0;
   double z_imag = 0.0;
   int iter = 0;

   while (z_real * z_real + z_imag * z_imag <= 4.0 && iter < max_iter) {
      double temp_real = z_real * z_real - z_imag * z_imag + real;
      z_imag = 2.0 * z_real * z_imag + imag;
      z_real = temp_real;
      iter++;
   }

   return iter;
}

int CALCULATED = 0;

template <size_t Size, size_t width, size_t height, size_t max_iter>
std::array<int, Size> calc_image(int begin = 0, int end = Size) {
   std::array<int, Size> result;

   if constexpr (Size <= 32) {
      for (int i = begin; i < end; i++) {
         const int x = i % width;
         const int y = i / width;

         const double real = (x - width / 2.0) * 4.0 / width;
         const double imag = (y - height / 2.0) * 2.0 / height;
         result[i - begin] = mandelbrot(real, imag, max_iter);
         CALCULATED++;
      }
   } else {
      constexpr size_t Win = Size / 4;
      static_assert(Win * 4 == Size);

      auto parts = ityr::parallel_invoke(
         [begin]{ return calc_image<Win, width, height, max_iter>(begin + 0 * Win, begin + 1 * Win); },
         [begin]{ return calc_image<Win, width, height, max_iter>(begin + 1 * Win, begin + 2 * Win); },
         [begin]{ return calc_image<Win, width, height, max_iter>(begin + 2 * Win, begin + 3 * Win); },
         [begin]{ return calc_image<Win, width, height, max_iter>(begin + 3 * Win, begin + 4 * Win); }
      );

      for (int i = 0; i < 4; i++) {
         for (int n = 0; n < Win; n++) {
            std::array<int, Win>* part =
               i == 0 ? &std::get<0>(parts) :
               i == 1 ? &std::get<1>(parts) :
               i == 2 ? &std::get<2>(parts) :
               i == 3 ? &std::get<3>(parts) : nullptr;
            int index = i * Win + n;
            result[index] = (*part)[n];
         }
      }
   }

   return result;
}

// Hauptprogramm
int main(int argc, char** argv) {
   constexpr int width = 256;
   constexpr int height = 256;
   constexpr int max_iter = 10'000;

   ityr::init();
   ityr::profiler_begin();

   ityr::root_exec([]() -> void {
      const auto result = calc_image<width * height, width, height, max_iter>();

      uint64_t sum = 0;
      for (auto v : result) {
         sum += v;
      }
      printf("SUM = %llu\nCALC = %d\n", sum, CALCULATED);
   });

   ityr::profiler_end();
   ityr::profiler_flush();
   ityr::fini();
   return 0;
}
