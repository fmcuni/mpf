import { authRouter } from "./router/auth";
import { mpfRouter } from "./router/mpf";
import { postRouter } from "./router/post";
import { createTRPCRouter } from "./trpc";

export const appRouter = createTRPCRouter({
  auth: authRouter,
  mpf: mpfRouter,
  post: postRouter,
});

// export type definition of API
export type AppRouter = typeof appRouter;
