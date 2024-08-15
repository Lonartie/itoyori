#include <array>
#include <stdio.h>
#include <cmath>
#include <cstdlib>
#include <ityr/ityr.hpp>

constexpr int N = 1'024;
constexpr float G = 6.67430e-11;
constexpr float dt = 0.01;

struct Vec3 {
   float x, y, z;

   Vec3& operator+=(const Vec3& other) {
      x += other.x;
      y += other.y;
      z += other.z;
      return *this;
   }

   Vec3& operator-=(const Vec3& other) {
      x -= other.x;
      y -= other.y;
      z -= other.z;
      return *this;
   }

   Vec3 operator*(float scalar) const { return {x * scalar, y * scalar, z * scalar}; }

   Vec3 operator+(const Vec3& other) const { return {x + other.x, y + other.y, z + other.z}; }
   Vec3 operator-(const Vec3& other) const { return {x - other.x, y - other.y, z - other.z}; }
};

struct Body {
   Vec3 position;
   Vec3 velocity;
   float mass;
};

Vec3 compute_gravitational_force(const Body& a, const Body& b) {
   const Vec3 diff = b.position - a.position;
   const double dist_sqr = diff.x * diff.x + diff.y * diff.y + diff.z * diff.z;
   const double dist = std::sqrt(dist_sqr);
   const double force_mag = G * a.mass * b.mass / dist_sqr;
   return diff * (force_mag / dist);
}

template <size_t Size = N>
std::array<Vec3, N> calc_forces(std::array<Body, N> bodies, int begin = 0, int end = Size) {
   std::array<Vec3, N> forces = {};

   if constexpr (Size <= 4) {
      for (int i = begin; i < end; i++) {
         for (int j = i + 1; j < N; ++j) {
            Vec3 force = compute_gravitational_force(bodies[i], bodies[j]);
            forces[i] += force;
            forces[j] -= force;
         }
      }
   } else {
      constexpr size_t Win = Size / 4;
      static_assert(Win * 4 == Size);

      auto parts = ityr::parallel_invoke(
         [begin, bodies]{ return calc_forces<Win>(bodies, begin + 0 * Win, begin + 1 * Win); },
         [begin, bodies]{ return calc_forces<Win>(bodies, begin + 1 * Win, begin + 2 * Win); },
         [begin, bodies]{ return calc_forces<Win>(bodies, begin + 2 * Win, begin + 3 * Win); },
         [begin, bodies]{ return calc_forces<Win>(bodies, begin + 3 * Win, begin + 4 * Win); }
      );

      for (int i = 0; i < 4; i++) {
         for (int n = 0; n < N; n++) {
            std::array<Vec3, N>* part =
               i == 0 ? &std::get<0>(parts) :
               i == 1 ? &std::get<1>(parts) :
               i == 2 ? &std::get<2>(parts) :
               i == 3 ? &std::get<3>(parts) : nullptr;
            forces[n] += (*part)[n];
         }
      }
   }

   return forces;
}

template <size_t Size = N>
std::array<Body, Size> calc_bodies(std::array<Body, N> bodies, std::array<Vec3, N> forces, int begin = 0, int end = Size) {
   std::array<Body, Size> result = {};

   if constexpr (Size <= 4) {
      for (int i = begin; i < end; i++) {
         Vec3 acceleration = forces[i] * (1.0 / bodies[i].mass);
         result[i - begin] = bodies[i];
         result[i - begin].velocity += acceleration * dt;
         result[i - begin].position += bodies[i].velocity * dt;
      }
   } else {
      constexpr size_t Win = Size / 4;
      static_assert(Win * 4 == Size);

      auto parts = ityr::parallel_invoke(
         [begin, bodies, forces]{ return calc_bodies<Win>(bodies, forces, begin + 0 * Win, begin + 1 * Win); },
         [begin, bodies, forces]{ return calc_bodies<Win>(bodies, forces, begin + 1 * Win, begin + 2 * Win); },
         [begin, bodies, forces]{ return calc_bodies<Win>(bodies, forces, begin + 2 * Win, begin + 3 * Win); },
         [begin, bodies, forces]{ return calc_bodies<Win>(bodies, forces, begin + 3 * Win, begin + 4 * Win); }
      );

      for (int i = 0; i < 4; i++) {
         for (int n = 0; n < Win; n++) {
            std::array<Body, Win>* part =
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

std::array<Body, N> update_bodies(std::array<Body, N> bodies) {
   std::array<Vec3, N> forces = calc_forces(bodies);
   return calc_bodies(bodies, forces);
}

int main(int argc, char** argv) {
   ityr::init();

   int loops = argc > 1 ? std::stoi(argv[1]) : 1'000;

   ityr::root_exec([loops]() {
      std::array<Body, N> bodies;
      std::srand(42);

      for (int i = 0; i < N; ++i) {
         Vec3 pos = {
            (float) (rand() % 150),
            (float) (rand() % 150),
            (float) (rand() % 150)
         };
         float mass = rand() % 100'000'000'000 + 100'000'000;
         bodies[i] = {pos, {0.0, 0.0, 0.0}, mass};
      }

      for (int step = 0; step < loops; ++step) {
         printf("iteration %d#", step);
         printf(" %f,%f,%f\n", bodies[0].position.x,bodies[0].position.y,bodies[0].position.z);
         bodies = update_bodies(bodies);
      }
   });

   ityr::fini();
   return 0;
}

// 66.062912,89.917801,130.708389